use anyhow::{Context, Result};
use std::{env, path::PathBuf};
use tarla_gateway::{router, GatewayConfig};

#[tokio::main]
async fn main() -> Result<()> {
    let dir = PathBuf::from(
        env::var("TARLA_WORKSPACE_DIR")
            .context("set TARLA_WORKSPACE_DIR to the paired workspace")?,
    );
    let core =
        env::var("TARLA_CORE_URL").context("set TARLA_CORE_URL to the workspace daemon origin")?;
    let token = env::var("TARLA_CORE_TOKEN").context("set TARLA_CORE_TOKEN")?;
    let cfg = GatewayConfig::from_env(&dir, &core, &token)?
        .context("enable one or more workspace gateways")?;
    let addr = env::var("TARLA_GATEWAY_LISTEN").unwrap_or_else(|_| "127.0.0.1:7701".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let app = axum::Router::new().nest("/gateway", router(cfg).await?);
    axum::serve(listener, app).await?;
    Ok(())
}
