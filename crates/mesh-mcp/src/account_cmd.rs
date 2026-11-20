//! `login`, `logout`, and `status`. Humans run these; MCP clients never do.

use mesh_pro::{ApiClient, DevicePoll, Entitlement, Store};
use std::time::{Duration, SystemTime};

const LOGIN_HELP: &str = "\
Usage:
  agent-mesh login [--token TOKEN] [--api-base URL]
  agent-mesh logout
  agent-mesh status

`login` without --token prints a device code and waits for you to approve it at the URL it
prints. The API base defaults to $AGENT_MESH_API_BASE or https://api.crewmesh.dev.

Logging in does not change the local mesh. Pro features turn on only when the account is entitled.
";

pub async fn login(
    mut args: impl Iterator<Item = String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut token = None;
    let mut api_base = std::env::var("AGENT_MESH_API_BASE").ok();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--token" => {
                token = Some(
                    args.next()
                        .ok_or("agent-mesh login: --token needs a value")?,
                );
            }
            "--api-base" => {
                api_base = Some(
                    args.next()
                        .ok_or("agent-mesh login: --api-base needs a URL")?,
                );
            }
            "--help" | "-h" => {
                print!("{LOGIN_HELP}");
                return Ok(());
            }
            other => {
                eprintln!("agent-mesh login: unrecognized argument `{other}`\n\n{LOGIN_HELP}");
                std::process::exit(2);
            }
        }
    }
    let api_base = api_base.unwrap_or_else(|| mesh_pro::DEFAULT_API_BASE.to_owned());
    let store = Store::from_env();
    let api = ApiClient::new()?;
    if let Some(token) = token {
        let ent = store.login_with_token(&api, &api_base, &token).await?;
        println!("logged in to {api_base}");
        print_entitlement(&ent);
        return Ok(());
    }

    let start = api.device_start(&api_base).await?;
    println!(
        "Open this URL and approve Crewmesh:\n  {}",
        start.verification_url
    );
    println!("Code: {}", start.user_code);
    println!("Waiting for approval...");
    let deadline = SystemTime::now() + Duration::from_secs(start.expires_in.max(1));
    let interval = Duration::from_secs(start.interval.max(1));
    loop {
        if SystemTime::now() >= deadline {
            eprintln!("login timed out; run `agent-mesh login` to try again");
            std::process::exit(1);
        }
        match api.device_poll(&api_base, &start.device_code).await? {
            DevicePoll::Pending { .. } => tokio::time::sleep(interval).await,
            DevicePoll::Expired => {
                eprintln!("that login code expired; run `agent-mesh login` to try again");
                std::process::exit(1);
            }
            approved @ DevicePoll::Approved { .. } => {
                let token = approved
                    .token()
                    .ok_or("the API approved the login without a token")?
                    .to_owned();
                let ent = approved
                    .entitlement()
                    .ok_or("the API approved the login without an entitlement")?;
                store.save_session(&api_base, &token, &ent)?;
                println!("logged in to {api_base}");
                print_entitlement(&ent);
                return Ok(());
            }
        }
    }
}

pub fn logout() -> Result<(), Box<dyn std::error::Error>> {
    mesh_pro::logout()?;
    println!("logged out. The local mesh is unchanged.");
    Ok(())
}

pub async fn status() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::from_env();
    if store.load().is_none() {
        println!("Not logged in. The local mesh is free and does not contact the network.");
        println!(
            "Crewmesh Pro is $9/mo or $90/yr. Run `agent-mesh login`, then upgrade at {}.",
            mesh_pro::upgrade_url()
        );
        std::process::exit(1);
    }
    let api = ApiClient::new()?;
    match store.status(&api).await {
        Ok(status) => {
            println!("Crewmesh account");
            println!("  api: {}", status.api_base.unwrap_or_default());
            if let Some(email) = &status.email {
                println!("  email: {email}");
            }
            if let Some(account) = &status.account_id {
                println!("  account: {account}");
            }
            match &status.plan {
                Some(plan) => println!("  plan: {plan}"),
                None => println!("  plan: unknown"),
            }
            println!("  pro: {}", if status.pro { "yes" } else { "no" });
            if status.offline {
                println!("  entitlement: offline, using the last cached answer");
            }
            if let Some(until) = status.cache_valid_until_unix {
                println!("  cache valid until unix {until}");
            }
            Ok(())
        }
        Err(err) => {
            eprintln!("agent-mesh status: {err}");
            std::process::exit(1);
        }
    }
}

fn print_entitlement(ent: &Entitlement) {
    println!("  account: {}", ent.account_id);
    if let Some(email) = &ent.email {
        println!("  email: {email}");
    }
    println!("  plan: {}", ent.plan);
    println!("  pro: {}", if ent.pro { "yes" } else { "no" });
}
