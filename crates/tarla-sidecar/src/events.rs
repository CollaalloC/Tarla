//! Real-time ordered `events.subscribe` stream client for `herdr-core` with `missed` resync.
use crate::{
    herdr::{Ipc, MAX_HERDR_FRAME_BYTES},
    invalid,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, io, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::time::timeout;

/// Upstream sends a heartbeat every 15s; fail closed if a stream goes silent past 30s.
const EVENT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HerdrEvent {
    pub seq: u64,
    pub kind: HerdrEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HerdrEventKind {
    Created {
        terminal: String,
        name: Option<String>,
        parent: Option<String>,
        labels: BTreeMap<String, String>,
    },
    Finished {
        terminal: String,
        exit_code: Option<i32>,
        reason: Option<String>,
    },
    /// Authoritative OSC 7501 state (`idle` | `working` | `done` | `blocked` | `error`).
    /// Never infers idle from `foreground.name` (Windows ConPTY Issue #70).
    Status {
        terminal: String,
        state: String,
        detail: Value,
    },
    Turn {
        terminal: String,
        turn: u64,
        state: String,
    },
    Labels {
        terminal: String,
        labels: BTreeMap<String, String>,
    },
    Doc {
        name: String,
        version: u64,
        value: Value,
    },
    Heartbeat,
    Missed {
        dropped: u64,
        snapshot_seq: u64,
        terminals: Vec<Value>,
    },
    Other {
        event: String,
        raw: Value,
    },
}

pub struct EventStream<S = Ipc> {
    reader: BufReader<S>,
    queued: Option<HerdrEvent>,
    last_seq: u64,
}

impl<S: AsyncRead + AsyncWrite + Unpin> EventStream<S> {
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub(crate) async fn connect(
        mut reader: BufReader<S>,
        since_seq: Option<u64>,
    ) -> io::Result<Self> {
        let mut params = json!({});
        if let Some(seq) = since_seq {
            params["since"] = json!(seq);
        }
        let mut req = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "events.subscribe",
            "params": params,
        }))?;
        req.push(b'\n');
        reader.get_mut().write_all(&req).await?;
        reader.get_mut().flush().await?;

        let raw = read_json_line(&mut reader).await?;
        if raw.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || raw.get("id").and_then(Value::as_u64) != Some(1)
        {
            return Err(invalid("unexpected events.subscribe handshake reply"));
        }
        if let Some(err) = raw.get("error") {
            return Err(io::Error::other(err.to_string()));
        }
        let result = raw
            .get("result")
            .ok_or_else(|| invalid("missing events.subscribe result"))?;
        let mut last_seq = result["seq"].as_u64().or(since_seq).unwrap_or(0);
        let queued = if result["missed"].as_bool() == Some(true) || result.get("dropped").is_some()
        {
            let snap_seq = result["seq"].as_u64().unwrap_or(last_seq);
            last_seq = last_seq.max(snap_seq);
            Some(HerdrEvent {
                seq: snap_seq,
                kind: HerdrEventKind::Missed {
                    dropped: result["dropped"].as_u64().unwrap_or(0),
                    snapshot_seq: snap_seq,
                    terminals: result["terminals"].as_array().cloned().unwrap_or_default(),
                },
            })
        } else {
            None
        };
        Ok(Self {
            reader,
            queued,
            last_seq,
        })
    }

    pub async fn next_event(&mut self) -> io::Result<HerdrEvent> {
        if let Some(ev) = self.queued.take() {
            return Ok(ev);
        }
        let line = timeout(EVENT_IDLE_TIMEOUT, read_json_line(&mut self.reader))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "herdr-core event stream missed heartbeat window",
                )
            })??;
        let parsed = parse_event_frame(&line, self.last_seq)?;
        if parsed.seq > 0 {
            if self.last_seq > 0 && parsed.seq < self.last_seq {
                return Err(invalid("non-monotonic herdr event sequence"));
            }
            self.last_seq = parsed.seq;
        }
        Ok(parsed)
    }
}

async fn read_json_line<S: AsyncRead + Unpin>(reader: &mut BufReader<S>) -> io::Result<Value> {
    let mut buf = Vec::new();
    let n = reader
        .take((MAX_HERDR_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut buf)
        .await?;
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "herdr-core event stream closed",
        ));
    }
    if buf.len() > MAX_HERDR_FRAME_BYTES || buf.last() != Some(&b'\n') {
        return Err(invalid("truncated or oversized event frame"));
    }
    serde_json::from_slice(&buf).map_err(|e| invalid(format!("invalid event JSON: {e}")))
}

fn parse_labels(v: &Value) -> BTreeMap<String, String> {
    v.as_object()
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

pub fn parse_event_frame(frame: &Value, fallback_seq: u64) -> io::Result<HerdrEvent> {
    let body = frame
        .get("params")
        .or_else(|| frame.get("result"))
        .unwrap_or(frame);
    let event_name = body["event"]
        .as_str()
        .or_else(|| body["kind"].as_str())
        .or_else(|| {
            frame["method"]
                .as_str()
                .and_then(|m| m.strip_prefix("events."))
        })
        .ok_or_else(|| invalid("missing event discriminator"))?;
    let seq = body["seq"].as_u64().unwrap_or(fallback_seq);

    let kind = match event_name {
        "heartbeat" | "ping" => HerdrEventKind::Heartbeat,
        "missed" => HerdrEventKind::Missed {
            dropped: body["dropped"].as_u64().unwrap_or(0),
            snapshot_seq: body["seq"].as_u64().unwrap_or(seq),
            terminals: body["terminals"].as_array().cloned().unwrap_or_default(),
        },
        "created" => HerdrEventKind::Created {
            terminal: body["terminal"]
                .as_str()
                .or_else(|| body["address"].as_str())
                .ok_or_else(|| invalid("created event missing terminal"))?
                .to_owned(),
            name: body["name"].as_str().map(str::to_owned),
            parent: body["parent"].as_str().map(str::to_owned),
            labels: parse_labels(&body["labels"]),
        },
        "finished" => HerdrEventKind::Finished {
            terminal: body["terminal"]
                .as_str()
                .or_else(|| body["address"].as_str())
                .ok_or_else(|| invalid("finished event missing terminal"))?
                .to_owned(),
            exit_code: body["exit_code"].as_i64().map(|c| c as i32),
            reason: body["reason"].as_str().map(str::to_owned),
        },
        "status" => {
            let terminal = body["terminal"]
                .as_str()
                .or_else(|| body["address"].as_str())
                .ok_or_else(|| invalid("status event missing terminal"))?
                .to_owned();
            let detail = body.get("status").cloned().unwrap_or_else(|| body.clone());
            let state = detail["state"]
                .as_str()
                .or_else(|| body["state"].as_str())
                .unwrap_or("unknown")
                .to_owned();
            HerdrEventKind::Status {
                terminal,
                state,
                detail,
            }
        }
        "turn" => HerdrEventKind::Turn {
            terminal: body["terminal"]
                .as_str()
                .or_else(|| body["address"].as_str())
                .ok_or_else(|| invalid("turn event missing terminal"))?
                .to_owned(),
            turn: body["turn"].as_u64().unwrap_or(0),
            state: body["state"].as_str().unwrap_or("idle").to_owned(),
        },
        "labels" => HerdrEventKind::Labels {
            terminal: body["terminal"]
                .as_str()
                .or_else(|| body["address"].as_str())
                .ok_or_else(|| invalid("labels event missing terminal"))?
                .to_owned(),
            labels: parse_labels(&body["labels"]),
        },
        "doc" => HerdrEventKind::Doc {
            name: body["name"]
                .as_str()
                .or_else(|| body["doc"].as_str())
                .ok_or_else(|| invalid("doc event missing name"))?
                .to_owned(),
            version: body["version"].as_u64().unwrap_or(0),
            value: body.get("value").cloned().unwrap_or(Value::Null),
        },
        other => HerdrEventKind::Other {
            event: other.to_owned(),
            raw: body.clone(),
        },
    };
    Ok(HerdrEvent { seq, kind })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn subscribes_ordered_events_parses_turn_osc_doc_and_missed_resync() {
        let (client, server) = duplex(4096);
        let upstream = tokio::spawn(async move {
            let mut s = BufReader::new(server);
            let mut line = String::new();
            s.read_line(&mut line).await.unwrap();
            let ask: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(ask["method"], "events.subscribe");
            assert_eq!(ask["params"]["since"], 10);
            s.get_mut()
                .write_all(
                    br#"{"jsonrpc":"2.0","id":1,"result":{"seq":14,"missed":true,"dropped":3,"terminals":[{"address":"t2","state":"live"}]}}"#,
                )
                .await
                .unwrap();
            s.get_mut().write_all(b"\n").await.unwrap();
            for frame in [
                r#"{"jsonrpc":"2.0","method":"events.event","params":{"seq":15,"event":"created","terminal":"t3","name":"rev","parent":"t2","labels":{"tarla_run":"r-1"}}}"#,
                r#"{"jsonrpc":"2.0","method":"events.event","params":{"seq":16,"event":"status","terminal":"t3","status":{"state":"working"},"foreground":{"name":"conhost.exe"}}}"#,
                r#"{"jsonrpc":"2.0","method":"events.event","params":{"seq":17,"event":"turn","terminal":"t3","turn":1,"state":"done"}}"#,
                r#"{"jsonrpc":"2.0","method":"events.event","params":{"seq":18,"event":"doc","name":"plan","version":2,"value":{"ok":true}}}"#,
                r#"{"jsonrpc":"2.0","method":"events.event","params":{"seq":19,"event":"heartbeat"}}"#,
                r#"{"jsonrpc":"2.0","method":"events.event","params":{"seq":20,"event":"finished","terminal":"t3","exit_code":0}}"#,
            ] {
                s.get_mut().write_all(frame.as_bytes()).await.unwrap();
                s.get_mut().write_all(b"\n").await.unwrap();
            }
        });

        let mut sub = EventStream::connect(BufReader::new(client), Some(10))
            .await
            .unwrap();
        assert!(matches!(
            sub.next_event().await.unwrap().kind,
            HerdrEventKind::Missed {
                dropped: 3,
                snapshot_seq: 14,
                ..
            }
        ));
        assert!(matches!(
            sub.next_event().await.unwrap().kind,
            HerdrEventKind::Created {
                ref terminal,
                parent: Some(ref p),
                ..
            } if terminal == "t3" && p == "t2"
        ));
        // Even with Windows ConPTY foreground.name present, OSC 7501 status is authoritative.
        assert!(matches!(
            sub.next_event().await.unwrap().kind,
            HerdrEventKind::Status {
                ref terminal,
                ref state,
                ..
            } if terminal == "t3" && state == "working"
        ));
        assert_eq!(
            sub.next_event().await.unwrap(),
            HerdrEvent {
                seq: 17,
                kind: HerdrEventKind::Turn {
                    terminal: "t3".into(),
                    turn: 1,
                    state: "done".into(),
                }
            }
        );
        assert!(matches!(
            sub.next_event().await.unwrap().kind,
            HerdrEventKind::Doc {
                ref name,
                version: 2,
                ..
            } if name == "plan"
        ));
        assert_eq!(
            sub.next_event().await.unwrap().kind,
            HerdrEventKind::Heartbeat
        );
        assert!(matches!(
            sub.next_event().await.unwrap().kind,
            HerdrEventKind::Finished {
                ref terminal,
                exit_code: Some(0),
                ..
            } if terminal == "t3"
        ));
        assert_eq!(sub.last_seq(), 20);
        upstream.await.unwrap();
    }
}
