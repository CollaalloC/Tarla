//! Slack Events / Block Kit and Discord application-command interactions.
use crate::{
    internal,
    platform::{matches_secret, Card, Event, Input, Route},
    store, Data, WebResult,
};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use ring::{hmac, signature};
use serde_json::{json, Value};

fn h<'a>(headers: &'a HeaderMap, key: &str) -> WebResult<&'a str> {
    headers
        .get(key)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)
}
fn fresh(time: &str) -> WebResult<()> {
    let t = time.parse::<i64>().map_err(|_| StatusCode::UNAUTHORIZED)?;
    if store::now().abs_diff(t) > 300 {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}
fn text(v: &Value, k: &str) -> WebResult<String> {
    v[k].as_str()
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .ok_or(StatusCode::BAD_REQUEST)
}
pub async fn slack(State(g): Data, headers: HeaderMap, body: Bytes) -> WebResult<Json<Value>> {
    let f = g.cfg.slack.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let time = h(&headers, "x-slack-request-timestamp")?;
    fresh(time)?;
    let mut signed = format!("v0:{time}:").into_bytes();
    signed.extend_from_slice(&body);
    let expected = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, f.signing.as_bytes()),
        &signed,
    );
    if !matches_secret(
        &format!("v0={}", hex::encode(expected.as_ref())),
        h(&headers, "x-slack-signature")?,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let v: Value = if headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.starts_with("application/x-www-form-urlencoded"))
    {
        #[derive(serde::Deserialize)]
        struct Form {
            payload: String,
        }
        let form: Form =
            serde_urlencoded::from_bytes(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
        serde_json::from_str(&form.payload)
    } else {
        serde_json::from_slice(&body)
    }
    .map_err(|_| StatusCode::BAD_REQUEST)?;
    if v["type"] == "url_verification" {
        return Ok(Json(json!({"challenge":text(&v,"challenge")?})));
    }
    let (event_id, chat, thread, actor, input) = if v["type"] == "event_callback" {
        if v["team_id"] != f.team {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let e = &v["event"];
        if e["type"] != "message" || !e["subtype"].is_null() || !e["bot_id"].is_null() {
            return Ok(Json(json!({})));
        }
        (
            text(&v, "event_id")?,
            text(e, "channel")?,
            e["thread_ts"].as_str().unwrap_or_default(),
            text(e, "user")?,
            Input::Message(text(e, "text")?),
        )
    } else if v["type"] == "block_actions" {
        if v["team"]["id"] != f.team {
            return Err(StatusCode::UNAUTHORIZED);
        }
        (
            text(&v, "trigger_id")?,
            text(&v["channel"], "id")?,
            v["message"]["thread_ts"].as_str().unwrap_or_default(),
            text(&v["user"], "id")?,
            Input::Button {
                value: text(&v["actions"][0], "value")?,
                callback_id: None,
                message_id: None,
            },
        )
    } else {
        return Ok(Json(json!({})));
    };
    let e = Event {
        event_id,
        route: Route {
            platform: "slack".into(),
            chat,
            thread: thread.into(),
            actor,
            reply_to: None,
        },
        input,
    };
    store::ingest(&mut *g.db().map_err(internal)?, e).map_err(internal)?;
    Ok(Json(json!({})))
}
pub async fn discord(State(g): Data, headers: HeaderMap, body: Bytes) -> WebResult<Json<Value>> {
    let f = g.cfg.discord.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let time = h(&headers, "x-signature-timestamp")?;
    fresh(time)?;
    let sig =
        hex::decode(h(&headers, "x-signature-ed25519")?).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let mut message = time.as_bytes().to_vec();
    message.extend_from_slice(&body);
    signature::UnparsedPublicKey::new(&signature::ED25519, &f.public)
        .verify(&message, &sig)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let v: Value = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    if v["application_id"] != f.app {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if v["type"] == 1 {
        return Ok(Json(json!({"type":1})));
    }
    let actor = v
        .pointer("/member/user/id")
        .or_else(|| v.pointer("/user/id"))
        .and_then(Value::as_str)
        .ok_or(StatusCode::BAD_REQUEST)?;
    let input = match v["type"].as_i64() {
        Some(2) if v["data"]["name"] == "tarla" => {
            let options = v["data"]["options"]
                .as_array()
                .ok_or(StatusCode::BAD_REQUEST)?;
            let t = options
                .iter()
                .find(|o| o["name"] == "text" && o["type"] == 3)
                .ok_or(StatusCode::BAD_REQUEST)?;
            Input::Message(text(t, "value")?)
        }
        Some(3) => Input::Button {
            value: text(&v["data"], "custom_id")?,
            callback_id: None,
            message_id: None,
        },
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    store::ingest(
        &mut *g.db().map_err(internal)?,
        Event {
            event_id: text(&v, "id")?,
            route: Route {
                platform: "discord".into(),
                chat: text(&v, "channel_id")?,
                thread: String::new(),
                actor: actor.into(),
                reply_to: None,
            },
            input,
        },
    )
    .map_err(internal)?;
    Ok(Json(
        json!({"type":4,"data":{"content":"Request received.","flags":64}}),
    ))
}
pub fn slack_payload(route: &Route, text: &str, card: Option<Card>) -> Value {
    let mut v = json!({"channel":route.chat,"text":text,"mrkdwn":false,"unfurl_links":false,"unfurl_media":false});
    if !route.thread.is_empty() {
        v["thread_ts"] = json!(route.thread)
    }
    if let Some(card) = card {
        v["text"] = json!(card.title);
        v["blocks"] = json!([{"type":"section","text":{"type":"plain_text","text":card.title}},
        {"type":"actions","elements":[{"type":"button","text":{"type":"plain_text","text":"Approve"},"action_id":"approve","value":format!("a.{}",card.token)},{"type":"button","text":{"type":"plain_text","text":"Reject"},"action_id":"reject","value":format!("r.{}",card.token)}]}]);
    }
    v
}
pub fn discord_payload(text: &str, card: Option<Card>) -> Value {
    let mut v = json!({"content":text,"allowed_mentions":{"parse":[]}});
    if let Some(card) = card {
        v["content"] = json!(card.title);
        v["components"] = json!([{"type":1,"components":[{"type":2,"style":3,"label":"Approve","custom_id":format!("a.{}",card.token)},{"type":2,"style":4,"label":"Reject","custom_id":format!("r.{}",card.token)}]}]);
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{Discord, GatewayConfig, Slack},
        Gateway,
    };
    use ring::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };
    use std::{
        path::Path,
        sync::{Arc, Mutex},
    };

    fn gateway(public: &[u8]) -> Arc<Gateway> {
        Arc::new(Gateway {
            cfg: GatewayConfig {
                database: ":memory:".into(),
                core: "http://127.0.0.1".parse().unwrap(),
                token: "owner".into(),
                telegram: None,
                feishu: None,
                slack: Some(Slack {
                    team: "T1".into(),
                    signing: "signing".into(),
                    bot: "unused".into(),
                }),
                discord: Some(Discord {
                    app: "123".into(),
                    public: public.to_vec(),
                    bot: "unused".into(),
                }),
            },
            connection: Mutex::new(store::open(Path::new(":memory:")).unwrap()),
            client: reqwest::Client::new(),
            feishu_token: Default::default(),
            instance: "test".into(),
            test_platform_origin: None,
        })
    }
    #[tokio::test]
    async fn slack_accepts_signed_challenge_within_window() {
        let g = gateway(&[]);
        let mut headers = HeaderMap::new();
        let time = store::now().to_string();
        let body = Bytes::from_static(br#"{"type":"url_verification","challenge":"hello"}"#);
        let mut payload = format!("v0:{time}:").into_bytes();
        payload.extend_from_slice(&body);
        let sig = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, b"signing"), &payload);
        headers.insert("x-slack-request-timestamp", time.parse().unwrap());
        headers.insert(
            "x-slack-signature",
            format!("v0={}", hex::encode(sig.as_ref())).parse().unwrap(),
        );
        assert_eq!(
            slack(State(g.clone()), headers.clone(), body.clone())
                .await
                .unwrap()
                .0["challenge"],
            "hello"
        );
        headers.insert("x-slack-request-timestamp", "0".parse().unwrap());
        assert_eq!(
            slack(State(g), headers, body).await.unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
    }
    #[tokio::test]
    async fn discord_requires_valid_signature_to_ack_ping() {
        let pk = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(pk.as_ref()).unwrap();
        let g = gateway(key.public_key().as_ref());
        let body = Bytes::from_static(br#"{"application_id":"123","type":1}"#);
        let now = store::now().to_string();
        let mut msg = now.as_bytes().to_vec();
        msg.extend_from_slice(&body);
        let mut h = HeaderMap::new();
        h.insert("x-signature-timestamp", now.parse().unwrap());
        h.insert(
            "x-signature-ed25519",
            hex::encode(key.sign(&msg).as_ref()).parse().unwrap(),
        );
        assert_eq!(
            discord(State(g.clone()), h.clone(), body.clone())
                .await
                .unwrap()
                .0["type"],
            1
        );
        h.remove("x-signature-ed25519");
        assert_eq!(
            discord(State(g), h, body).await.unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
    }
}
