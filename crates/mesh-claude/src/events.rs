//! Claude's `stream-json` event shapes and on-disk transcript format.

use mesh_core::{CostMicros, Speaker, Transcript, Turn};
use serde::Deserialize;

/// One line of `--output-format stream-json`. Only the terminal `result` event matters for a
/// prompt turn; the intermediate `assistant`/`system` events are progress reporting.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum StreamEvent {
    #[serde(rename = "result")]
    Result(ResultEnvelope),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultEnvelope {
    #[serde(default)]
    pub result: String,
    #[serde(default)]
    pub is_error: bool,
    /// Present only when Claude reports spend. `None` means unreported, never free.
    #[serde(default, rename = "total_cost_usd")]
    pub total_cost_usd: Option<f64>,
    #[serde(default)]
    pub usage: UsageEnvelope,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Claude splits the input count across three fields. `input_tokens` alone is only the
/// uncached portion, which on a resumed session collapses to single digits while tens of
/// thousands of cached tokens go unreported. Cached reads are billed, just more cheaply, so all
/// three belong in the total.
#[derive(Debug, Default, Deserialize)]
pub struct UsageEnvelope {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
}

impl UsageEnvelope {
    /// Every token the model read this turn, cached or not.
    pub fn total_input(&self) -> u64 {
        self.input_tokens
            .saturating_add(self.cache_creation_input_tokens)
            .saturating_add(self.cache_read_input_tokens)
    }
}

/// Convert dollars to integer micros. Held as an integer because summing floats across many turns
/// will not reconcile against a vendor's usage export.
pub fn usd_to_micros(usd: f64) -> CostMicros {
    CostMicros((usd * 1_000_000.0).round().max(0.0) as u64)
}

/// One line of Claude's on-disk transcript (`~/.claude/projects/<slug>/<session>.jsonl`).
#[derive(Debug, Deserialize)]
struct TranscriptLine {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    message: Option<TranscriptMessage>,
}

#[derive(Debug, Deserialize)]
struct TranscriptMessage {
    #[serde(default)]
    role: String,
    /// Content is either a bare string or an array of typed blocks depending on the writer.
    #[serde(default)]
    content: serde_json::Value,
}

/// Parse a transcript, keeping only user and assistant prose. Lines that fail to parse are
/// skipped: the file also holds summaries, tool records and schema from older CLI versions, and
/// one unknown line must not lose the whole conversation.
pub fn parse_transcript(raw: &str) -> Transcript {
    Transcript::from_turns(raw.lines().filter_map(|line| {
        let parsed: TranscriptLine = serde_json::from_str(line).ok()?;
        let speaker = match parsed.kind.as_str() {
            "user" => Speaker::User,
            "assistant" => Speaker::Agent,
            _ => return None,
        };
        let message = parsed.message?;
        // Trust the envelope's own role when it disagrees with the line type.
        let speaker = match message.role.as_str() {
            "user" => Speaker::User,
            "assistant" => Speaker::Agent,
            _ => speaker,
        };
        let text = content_text(&message.content);
        (!text.trim().is_empty()).then_some(Turn { speaker, text })
    }))
}

fn content_text(content: &serde_json::Value) -> String {
    match content {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(blocks) => blocks
            .iter()
            .filter(|b| b.get("type").and_then(serde_json::Value::as_str) == Some("text"))
            .filter_map(|b| b.get("text").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_result_event() {
        let line = r#"{"type":"result","subtype":"success","is_error":false,
            "result":"MANGO","total_cost_usd":0.0123,
            "usage":{"input_tokens":10,"output_tokens":4},"session_id":"abc"}"#;

        match serde_json::from_str::<StreamEvent>(line).expect("parse") {
            StreamEvent::Result(r) => {
                assert_eq!(r.result, "MANGO");
                assert!(!r.is_error);
                assert_eq!(r.total_cost_usd, Some(0.0123));
                assert_eq!(r.usage.output_tokens, 4);
            }
            StreamEvent::Other => panic!("expected a result event"),
        }
    }

    /// Recorded verbatim from a live resumed session. The uncached `input_tokens` is 4 while the
    /// turn really read ~58k tokens; the extra fields must deserialize or the count is lost.
    #[test]
    fn parses_real_usage_envelope_with_cache_fields() {
        let line = r#"{"type":"result","subtype":"success","is_error":false,"result":"C",
            "total_cost_usd":0.02908775,
            "usage":{"input_tokens":4,"cache_creation_input_tokens":11,
                     "cache_read_input_tokens":57848,"output_tokens":3,
                     "service_tier":"standard","speed":"standard"}}"#;

        match serde_json::from_str::<StreamEvent>(line).expect("parse") {
            StreamEvent::Result(r) => {
                assert_eq!(r.usage.cache_read_input_tokens, 57_848);
                assert_eq!(r.usage.total_input(), 57_863);
            }
            StreamEvent::Other => panic!("expected a result event"),
        }
    }

    /// An envelope with no cache fields at all must still report its uncached count.
    #[test]
    fn total_input_falls_back_to_plain_input_tokens() {
        let usage = UsageEnvelope {
            input_tokens: 2643,
            ..Default::default()
        };
        assert_eq!(usage.total_input(), 2643);
    }

    /// Progress events must not be mistaken for the terminal result, or a prompt would return
    /// before the agent finished speaking.
    #[test]
    fn non_result_events_are_other() {
        for line in [
            r#"{"type":"system","subtype":"init","session_id":"x"}"#,
            r#"{"type":"assistant","message":{"role":"assistant","content":[]}}"#,
        ] {
            assert!(matches!(
                serde_json::from_str::<StreamEvent>(line).expect("parse"),
                StreamEvent::Other
            ));
        }
    }

    #[test]
    fn usd_converts_to_micros() {
        assert_eq!(usd_to_micros(0.0123), CostMicros(12_300));
        assert_eq!(usd_to_micros(0.0), CostMicros(0));
        // Sub-micro amounts round rather than truncate to a misleading zero.
        assert_eq!(usd_to_micros(0.0000006), CostMicros(1));
    }

    #[test]
    fn parses_string_and_block_content() {
        let raw = concat!(
            r#"{"type":"user","message":{"role":"user","content":"hello"}}"#,
            "\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"hi there"}]}}"#,
        );

        let transcript = parse_transcript(raw);

        assert_eq!(
            transcript.turns,
            vec![
                Turn { speaker: Speaker::User, text: "hello".to_owned() },
                Turn { speaker: Speaker::Agent, text: "hi there".to_owned() },
            ]
        );
    }

    /// Real transcripts carry summary lines, tool results and unparseable records from older CLI
    /// versions. One bad line must not discard the conversation around it.
    #[test]
    fn skips_noise_without_losing_conversation() {
        let raw = concat!(
            r#"{"type":"summary","summary":"API Error","leafUuid":"x"}"#,
            "\n",
            "not json at all\n",
            r#"{"type":"user","message":{"role":"user","content":"keep me"}}"#,
            "\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"t"}]}}"#,
        );

        let transcript = parse_transcript(raw);

        assert_eq!(
            transcript.turns,
            vec![Turn { speaker: Speaker::User, text: "keep me".to_owned() }],
            "tool-only and unparseable lines contribute nothing, prose survives"
        );
    }

    #[test]
    fn empty_transcript_is_empty() {
        assert!(parse_transcript("").is_empty());
    }
}
