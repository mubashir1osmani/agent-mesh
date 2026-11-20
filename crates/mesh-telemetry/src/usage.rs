//! Per-agent token and cost accounting.
//!
//! Cost is deliberately awkward here, and that is the point: of the agents the mesh drives, only
//! claude and grok report spend at all. An absent cost must stay absent rather than being summed as
//! zero, or a dashboard would show a confidently wrong "$0.00" for agents that simply never said.

use mesh_core::{AgentId, CostMicros, Reply, Usage};
use std::collections::BTreeMap;
use std::sync::RwLock;

/// Running totals for one agent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentUsage {
    pub turns: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Summed spend, in integer micros. Integer because summing float dollars across many turns
    /// will not reconcile against a vendor's own usage export.
    pub cost_micros: u64,
    /// Turns where the agent reported no cost. Tracked so a total can be read honestly: a small
    /// `cost_micros` alongside a large `turns_without_cost` means unreported, not cheap.
    pub turns_without_cost: u64,
}

impl AgentUsage {
    pub fn total_tokens(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }

    /// Spend as dollars, or `None` when *no* turn reported a cost, so callers cannot render an
    /// unreported total as `$0.00`.
    pub fn cost_usd(&self) -> Option<f64> {
        if self.turns_without_cost == self.turns {
            return None;
        }
        Some(CostMicros(self.cost_micros).as_usd())
    }

    /// Is every turn's cost accounted for?
    pub fn cost_is_complete(&self) -> bool {
        self.turns_without_cost == 0
    }
}

/// Accumulates usage across agents and emits metrics as it goes.
#[derive(Debug, Default)]
pub struct UsageRecorder {
    totals: RwLock<BTreeMap<AgentId, AgentUsage>>,
}

impl UsageRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one completed turn.
    pub fn record(&self, agent: &AgentId, reply: &Reply) {
        self.add(agent, &reply.usage, reply.cost);

        let agent_label = agent.to_string();
        metrics::counter!(
            crate::metrics_names::TOKENS_TOTAL,
            "agent" => agent_label.clone(),
            "direction" => "input",
        )
        .increment(reply.usage.input_tokens);
        metrics::counter!(
            crate::metrics_names::TOKENS_TOTAL,
            "agent" => agent_label.clone(),
            "direction" => "output",
        )
        .increment(reply.usage.output_tokens);

        // Only emitted when the agent actually reported spend, so the metric's absence is
        // meaningful rather than a zero that looks like free usage.
        if let Some(cost) = reply.cost {
            metrics::counter!(
                crate::metrics_names::COST_MICROS_TOTAL,
                "agent" => agent_label,
            )
            .increment(cost.0);
        }
    }

    fn add(&self, agent: &AgentId, usage: &Usage, cost: Option<CostMicros>) {
        let mut guard = self.totals.write().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(agent.clone()).or_default();
        entry.turns += 1;
        entry.input_tokens += usage.input_tokens;
        entry.output_tokens += usage.output_tokens;
        match cost {
            Some(c) => entry.cost_micros += c.0,
            None => entry.turns_without_cost += 1,
        }
    }

    pub fn get(&self, agent: &AgentId) -> AgentUsage {
        self.totals
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(agent)
            .copied()
            .unwrap_or_default()
    }

    pub fn all(&self) -> BTreeMap<AgentId, AgentUsage> {
        self.totals
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn reply(input: u64, output: u64, cost: Option<u64>) -> Reply {
        Reply {
            text: "hi".to_owned(),
            usage: Usage {
                input_tokens: input,
                output_tokens: output,
                total_tokens: input + output,
            },
            cost: cost.map(CostMicros),
        }
    }

    #[test]
    fn accumulates_tokens_across_turns() {
        let rec = UsageRecorder::new();
        let agent = AgentId::new("codex");

        rec.record(&agent, &reply(100, 10, None));
        rec.record(&agent, &reply(50, 5, None));

        let usage = rec.get(&agent);
        assert_eq!(usage.turns, 2);
        assert_eq!(usage.input_tokens, 150);
        assert_eq!(usage.output_tokens, 15);
        assert_eq!(usage.total_tokens(), 165);
    }

    /// The core honesty requirement: an agent that never reports cost must read as unknown, not
    /// as free. Returning `Some(0.0)` here would put a confidently wrong $0.00 on a dashboard.
    #[test]
    fn cost_is_unknown_not_zero_when_never_reported() {
        let rec = UsageRecorder::new();
        let agent = AgentId::new("codex");

        rec.record(&agent, &reply(100, 10, None));
        rec.record(&agent, &reply(100, 10, None));

        let usage = rec.get(&agent);
        assert_eq!(usage.cost_usd(), None, "unreported cost must not read as $0");
        assert!(!usage.cost_is_complete());
    }

    #[test]
    fn cost_sums_when_reported() {
        let rec = UsageRecorder::new();
        let agent = AgentId::new("claude");

        rec.record(&agent, &reply(10, 1, Some(12_300)));
        rec.record(&agent, &reply(10, 1, Some(7_700)));

        let usage = rec.get(&agent);
        assert_eq!(usage.cost_micros, 20_000);
        assert_eq!(usage.cost_usd(), Some(0.02));
        assert!(usage.cost_is_complete());
    }

    /// A partially-reported total is the trickiest case: report what is known, but flag that the
    /// figure is incomplete so it is not mistaken for the full bill.
    #[test]
    fn partial_cost_is_reported_but_flagged_incomplete() {
        let rec = UsageRecorder::new();
        let agent = AgentId::new("grok");

        rec.record(&agent, &reply(10, 1, Some(5_000)));
        rec.record(&agent, &reply(10, 1, None));

        let usage = rec.get(&agent);
        assert_eq!(usage.cost_usd(), Some(0.005), "known spend is still reported");
        assert!(
            !usage.cost_is_complete(),
            "but the caller must be able to tell it is not the whole bill"
        );
        assert_eq!(usage.turns_without_cost, 1);
    }

    /// Agents must not pool into one another's totals, or per-agent attribution is meaningless.
    #[test]
    fn agents_are_accounted_separately() {
        let rec = UsageRecorder::new();
        rec.record(&AgentId::new("claude"), &reply(100, 10, Some(1_000)));
        rec.record(&AgentId::new("codex"), &reply(7, 3, None));

        assert_eq!(rec.get(&AgentId::new("claude")).input_tokens, 100);
        assert_eq!(rec.get(&AgentId::new("codex")).input_tokens, 7);
        assert_eq!(rec.all().len(), 2);
    }

    #[test]
    fn unknown_agent_reports_zeroed_usage() {
        let rec = UsageRecorder::new();
        let usage = rec.get(&AgentId::new("never-used"));
        assert_eq!(usage, AgentUsage::default());
        assert_eq!(usage.cost_usd(), None);
    }
}
