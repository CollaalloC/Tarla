use crate::config::Feishu;
use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};
use anyhow::{anyhow, bail, ensure, Context, Result};
use axum::http::HeaderMap;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

pub fn matches_secret(a: &str, b: &str) -> bool {
    bool::from(Sha256::digest(a.as_bytes()).ct_eq(&Sha256::digest(b.as_bytes())))
}

pub fn verified_feishu(cfg: &Feishu, headers: &HeaderMap, body: &[u8], now: i64) -> Result<Value> {
    let mut payload: Value = serde_json::from_slice(body)?;
    if let Some(key) = &cfg.encrypt_key {
        let encrypted = STANDARD.decode(
            payload["encrypt"]
                .as_str()
                .context("encrypted event required")?,
        )?;
        ensure!(encrypted.len() >= 32, "invalid encrypted body");
        let hash = Sha256::digest(key.as_bytes());
        let cipher = cbc::Decryptor::<aes::Aes256>::new_from_slices(&hash, &encrypted[..16])?;
        payload = serde_json::from_slice(
            &cipher
                .decrypt_padded_vec_mut::<Pkcs7>(&encrypted[16..])
                .map_err(|_| anyhow!("invalid padding"))?,
        )?;
        if payload["type"] != "url_verification" {
            let get = |k: &str| {
                headers
                    .get(k)
                    .and_then(|v| v.to_str().ok())
                    .context("missing signature headers")
            };
            let time = get("x-lark-request-timestamp")?;
            ensure!(now.abs_diff(time.parse::<i64>()?) <= 300, "stale signature");
            let mut digest = Sha256::new();
            for part in [
                time.as_bytes(),
                get("x-lark-request-nonce")?.as_bytes(),
                key.as_bytes(),
                body,
            ] {
                digest.update(part)
            }
            ensure!(
                matches_secret(
                    &format!("{:x}", digest.finalize()),
                    get("x-lark-signature")?
                ),
                "invalid signature"
            );
        }
    }
    let token = payload
        .pointer("/header/token")
        .or_else(|| payload.get("token"))
        .and_then(Value::as_str);
    ensure!(
        token.is_some_and(|t| matches_secret(t, &cfg.verification_token)),
        "invalid verification token"
    );
    if payload["type"] != "url_verification" {
        ensure!(payload["header"]["app_id"] == cfg.app_id, "wrong app")
    }
    Ok(payload)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Route {
    pub platform: String,
    pub chat: String,
    pub thread: String,
    pub actor: String,
    /// First message in a Feishu thread. Needed to reply into the same thread.
    pub reply_to: Option<String>,
}
#[derive(Serialize, Deserialize, Debug)]
pub enum Input {
    Message(String),
    Button {
        value: String,
        callback_id: Option<String>,
        message_id: Option<String>,
    },
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Event {
    pub event_id: String,
    pub route: Route,
    pub input: Input,
}
fn s(v: &Value, k: &str) -> Result<String> {
    v[k].as_str()
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("missing {k}"))
}
fn id(v: &Value, k: &str) -> Result<String> {
    v[k].as_i64()
        .map(|n| n.to_string())
        .with_context(|| format!("missing {k}"))
}

pub fn telegram(v: Value) -> Result<Option<Event>> {
    let event_id = id(&v, "update_id")?;
    let (msg, actor, input) = if let Some(q) = v.get("callback_query") {
        if q["from"]["is_bot"] == true {
            return Ok(None);
        }
        (
            &q["message"],
            id(&q["from"], "id")?,
            Input::Button {
                value: s(q, "data")?,
                callback_id: Some(s(q, "id")?),
                message_id: None,
            },
        )
    } else if let Some(m) = v.get("message") {
        if m["from"]["is_bot"] == true || m["text"].as_str().is_none() {
            return Ok(None);
        }
        (m, id(&m["from"], "id")?, Input::Message(s(m, "text")?))
    } else {
        return Ok(None);
    };
    let route = Route {
        platform: "telegram".into(),
        chat: id(&msg["chat"], "id")?,
        thread: msg["message_thread_id"]
            .as_i64()
            .map(|id| id.to_string())
            .unwrap_or_default(),
        actor,
        reply_to: None,
    };
    Ok(Some(Event {
        event_id,
        route,
        input,
    }))
}
pub fn feishu(v: &Value) -> Result<Option<Event>> {
    let header = &v["header"];
    let e = &v["event"];
    let (route, input) = match header["event_type"].as_str() {
        Some("im.message.receive_v1") => {
            let m = &e["message"];
            if m["message_type"] != "text" || e["sender"]["sender_type"] != "user" {
                return Ok(None);
            }
            let content: Value = serde_json::from_str(&s(m, "content")?)?;
            let thread = m["thread_id"]
                .as_str()
                .filter(|v| !v.is_empty())
                .or_else(|| m["root_id"].as_str().filter(|v| !v.is_empty()))
                .unwrap_or_default();
            let reply_to = if thread.is_empty() {
                None
            } else {
                Some(
                    m["root_id"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .unwrap_or(m["message_id"].as_str().context("missing message_id")?)
                        .to_owned(),
                )
            };
            (
                Route {
                    platform: "feishu".into(),
                    chat: s(m, "chat_id")?,
                    thread: thread.into(),
                    actor: s(&e["sender"]["sender_id"], "open_id")?,
                    reply_to,
                },
                Input::Message(s(&content, "text")?),
            )
        }
        Some("card.action.trigger") => {
            let actor = s(&e["operator"], "open_id")?;
            (
                Route {
                    platform: "feishu".into(),
                    chat: s(&e["context"], "open_chat_id")?,
                    thread: String::new(),
                    actor,
                    reply_to: None,
                },
                Input::Button {
                    value: s(&e["action"]["value"], "tarla")?,
                    callback_id: None,
                    message_id: Some(s(&e["context"], "open_message_id")?),
                },
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(Event {
        event_id: s(header, "event_id")?,
        route,
        input,
    }))
}

pub struct Card<'a> {
    pub title: &'a str,
    pub token: &'a str,
}
pub fn telegram_payload(route: &Route, text: &str, card: Option<Card>) -> Result<Value> {
    let mut b = json!({"chat_id":route.chat,"text":text});
    if !route.thread.is_empty() {
        b["message_thread_id"] = json!(route.thread.parse::<i64>()?)
    }
    if let Some(c) = card {
        b["text"] = json!(c.title);
        b["reply_markup"] = json!({"inline_keyboard":[[{"text":"Approve","callback_data":format!("a.{}",c.token)},{"text":"Reject","callback_data":format!("r.{}",c.token)}]]})
    }
    Ok(b)
}
pub fn feishu_payload(route: &Route, text: &str, card: Option<Card>) -> Value {
    let (kind, content) = match card {
        Some(c) => (
            "interactive",
            json!({"elements":[{"tag":"div","text":{"tag":"plain_text","content":c.title}},
        {"tag":"action","actions":[{"tag":"button","text":{"tag":"plain_text","content":"Approve"},"type":"primary","value":{"tarla":format!("a.{}",c.token)}},
        {"tag":"button","text":{"tag":"plain_text","content":"Reject"},"value":{"tarla":format!("r.{}",c.token)}}]}]}),
        ),
        None => ("text", json!({"text":text})),
    };
    let mut b = json!({"msg_type":kind,"content":content.to_string()});
    if route.reply_to.is_some() {
        b["reply_in_thread"] = json!(true)
    } else {
        b["receive_id"] = json!(route.chat)
    }
    b
}
pub fn decision(v: &str) -> Result<(&str, &str)> {
    let (k, t) = v.split_once('.').context("invalid action")?;
    ensure!(
        t.len() == 32 && t.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid card token"
    );
    match k {
        "a" => Ok(("approved", t)),
        "r" => Ok(("rejected", t)),
        _ => bail!("unknown action"),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn topic_and_callback_actor() {
        let e=telegram(json!({"update_id":22,"callback_query":{"id":"q","from":{"id":7},"message":{"chat":{"id":-1},"from":{"id":99},"message_thread_id":3},"data":"a.123"}})).unwrap().unwrap();
        assert_eq!(
            (e.route.actor.as_str(), e.route.thread.as_str()),
            ("7", "3")
        );
        assert_eq!(
            telegram_payload(&e.route, "reply", None).unwrap()["message_thread_id"],
            3
        );
    }
    #[test]
    fn feishu_rejects_other_app_and_preserves_thread() {
        let cfg = Feishu {
            app_id: "app".into(),
            app_secret: "secret".into(),
            verification_token: "t".into(),
            encrypt_key: None,
        };
        let mut v = json!({"header":{"token":"t","app_id":"other","event_type":"im.message.receive_v1","event_id":"evt"},"event":{
            "message":{"chat_id":"oc","thread_id":"th","root_id":"om_root","message_id":"om_reply","message_type":"text","content":"{\"text\":\"hello\"}"},
            "sender":{"sender_type":"user","sender_id":{"open_id":"ou_7"}}}});
        assert!(verified_feishu(&cfg, &HeaderMap::new(), v.to_string().as_bytes(), 0).is_err());
        v["header"]["app_id"] = json!("app");
        let ev =
            feishu(&verified_feishu(&cfg, &HeaderMap::new(), v.to_string().as_bytes(), 0).unwrap())
                .unwrap()
                .unwrap();
        assert_eq!(ev.route.reply_to.as_deref(), Some("om_root"));
        assert_eq!(ev.route.thread, "th");
    }
}
