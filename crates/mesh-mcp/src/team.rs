use mesh_pro::RelayClient;

pub async fn run(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn std::error::Error>> {
    let command = args.next().unwrap_or_default();
    match command.as_str() {
        "create" => {
            let name = args.next().ok_or("agent-mesh team create needs a name")?;
            let team = client()?.create_team(&name).await?;
            println!("{} {}", team.name, team.id);
            Ok(())
        }
        "invite" => {
            let email = args.next().ok_or("agent-mesh team invite needs an email")?;
            let team = client()?.invite(&email).await?;
            println!("{}", team.members.len());
            Ok(())
        }
        other => {
            eprintln!("agent-mesh team: unrecognized argument `{other}`");
            std::process::exit(2);
        }
    }
}

pub async fn allow(
    mut args: impl Iterator<Item = String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let email = args.next().ok_or("agent-mesh allow needs an email")?;
    client()?.allow_email(&email).await?;
    println!("allowed");
    Ok(())
}

fn client() -> Result<RelayClient, Box<dyn std::error::Error>> {
    let creds = mesh_pro::Store::from_env().load().ok_or("not logged in")?;
    RelayClient::new(creds.api_base, creds.token, "local").map_err(Into::into)
}
