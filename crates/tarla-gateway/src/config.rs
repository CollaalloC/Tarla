use anyhow::{bail, Context, Result};
use reqwest::Url;
use serde::Deserialize;
use std::{
    env, fs,
    path::{Path, PathBuf},
};

pub struct GatewayConfig {
    pub(crate) database: PathBuf,
    pub(crate) core: Url,
    pub(crate) token: String,
    pub(crate) telegram: Option<Telegram>,
    pub(crate) feishu: Option<Feishu>,
    pub(crate) slack: Option<Slack>,
    pub(crate) discord: Option<Discord>,
}
pub(crate) struct Telegram {
    pub token: String,
    pub secret: String,
}
pub(crate) struct Feishu {
    pub app_id: String,
    pub app_secret: String,
    pub verification_token: String,
    pub encrypt_key: Option<String>,
}

pub(crate) struct Slack {
    pub team: String,
    pub signing: String,
    pub bot: String,
}
pub(crate) struct Discord {
    pub app: String,
    pub public: Vec<u8>,
    pub bot: String,
}
#[derive(Deserialize)]
struct SlackSettings {
    #[serde(default)]
    enabled: bool,
    team_id: String,
    signing_secret_env: Option<String>,
    bot_token_env: Option<String>,
}
#[derive(Deserialize)]
struct DiscordSettings {
    #[serde(default)]
    enabled: bool,
    application_id: String,
    public_key: String,
    bot_token_env: Option<String>,
}
#[derive(Deserialize, Default)]
struct File {
    #[serde(default)]
    gateway: Channels,
}
#[derive(Deserialize, Default)]
struct Channels {
    telegram: Option<TgSettings>,
    feishu: Option<FsSettings>,
    slack: Option<SlackSettings>,
    discord: Option<DiscordSettings>,
}
#[derive(Deserialize)]
struct TgSettings {
    #[serde(default)]
    enabled: bool,
    bot_token_env: Option<String>,
    webhook_secret_env: Option<String>,
}
#[derive(Deserialize)]
struct FsSettings {
    #[serde(default)]
    enabled: bool,
    app_id: Option<String>,
    app_secret_env: Option<String>,
    verification_token_env: Option<String>,
    encrypt_key_env: Option<String>,
}
fn secret(name: Option<&str>, default: &str) -> Result<String> {
    let var = name.unwrap_or(default);
    let s = env::var(var).with_context(|| format!("missing gateway credential in {var}"))?;
    if s.is_empty() {
        bail!("empty gateway credential in {var}")
    }
    Ok(s)
}
impl GatewayConfig {
    pub fn from_env(workspace_dir: &Path, core_url: &str, token: &str) -> Result<Option<Self>> {
        let cfg: File = toml::from_str(&fs::read_to_string(workspace_dir.join("workspace.toml"))?)?;
        let telegram = cfg
            .gateway
            .telegram
            .filter(|s| s.enabled)
            .map(|s| -> Result<_> {
                let token = secret(s.bot_token_env.as_deref(), "TARLA_TELEGRAM_BOT_TOKEN")?;
                if !token.contains(':')
                    || !token
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b":_-".contains(&b))
                {
                    bail!("invalid Telegram bot token format")
                }
                Ok(Telegram {
                    token,
                    secret: secret(
                        s.webhook_secret_env.as_deref(),
                        "TARLA_TELEGRAM_WEBHOOK_SECRET",
                    )?,
                })
            })
            .transpose()?;
        let feishu = cfg
            .gateway
            .feishu
            .filter(|s| s.enabled)
            .map(|s| -> Result<_> {
                Ok(Feishu {
                    app_id: s.app_id.context("missing gateway.feishu.app_id")?,
                    app_secret: secret(s.app_secret_env.as_deref(), "TARLA_FEISHU_APP_SECRET")?,
                    verification_token: secret(
                        s.verification_token_env.as_deref(),
                        "TARLA_FEISHU_VERIFICATION_TOKEN",
                    )?,
                    encrypt_key: s
                        .encrypt_key_env
                        .map(|v| secret(Some(&v), ""))
                        .transpose()?,
                })
            })
            .transpose()?;
        let slack = cfg
            .gateway
            .slack
            .filter(|s| s.enabled)
            .map(|s| -> Result<_> {
                Ok(Slack {
                    team: s.team_id,
                    signing: secret(
                        s.signing_secret_env.as_deref(),
                        "TARLA_SLACK_SIGNING_SECRET",
                    )?,
                    bot: secret(s.bot_token_env.as_deref(), "TARLA_SLACK_BOT_TOKEN")?,
                })
            })
            .transpose()?;
        let discord = cfg
            .gateway
            .discord
            .filter(|s| s.enabled)
            .map(|s| -> Result<_> {
                let public = hex::decode(s.public_key)?;
                anyhow::ensure!(public.len() == 32, "invalid Discord public key");
                Ok(Discord {
                    app: s.application_id,
                    public,
                    bot: secret(s.bot_token_env.as_deref(), "TARLA_DISCORD_BOT_TOKEN")?,
                })
            })
            .transpose()?;
        if telegram.is_none() && feishu.is_none() && slack.is_none() && discord.is_none() {
            return Ok(None);
        }
        if token.is_empty() {
            bail!("empty Core bearer token")
        }
        let core = Url::parse(core_url)?;
        if !matches!(core.scheme(), "http" | "https")
            || !core.username().is_empty()
            || core.password().is_some()
        {
            bail!("invalid core origin")
        }
        if core.scheme() == "http"
            && !matches!(core.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
        {
            bail!("external core origin requires HTTPS")
        }
        Ok(Some(Self {
            database: workspace_dir.join("gateway.db"),
            core,
            token: token.to_owned(),
            telegram,
            feishu,
            slack,
            discord,
        }))
    }
}
