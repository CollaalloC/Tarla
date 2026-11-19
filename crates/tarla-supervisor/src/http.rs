use crate::{BrainBackend, BrainError, BrainTurnOutput, ChatMessage, ToolCall, ToolSpec};
use anyhow::{anyhow, bail, ensure, Context, Result};
use async_trait::async_trait;
use reqwest::{Client, Url};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

const MAX_PREVIEW: usize = 8192;
const MAX_RESPONSE: usize = 1024 * 1024;

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Dialect {
    Openai,
    Anthropic,
}

pub(crate) struct HttpBrain {
    dialect: Dialect,
    client: Client,
    endpoint: Url,
    model: String,
    key_env: Option<String>,
}
impl HttpBrain {
    pub(crate) fn new(
        dialect: Dialect,
        endpoint: &str,
        model: &str,
        key_env: Option<String>,
        proxy: Option<&str>,
    ) -> Result<Self> {
        let endpoint = Url::parse(endpoint)?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https")
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none(),
            "invalid brain endpoint; put credentials in api_key_env"
        );
        ensure!(!model.trim().is_empty(), "brain model name is required");
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(10));
        if let Some(proxy) = proxy {
            ensure!(
                matches!(Url::parse(proxy)?.scheme(), "socks5h" | "socks5"),
                "expected workspace SOCKS5 endpoint"
            );
            builder = builder.proxy(reqwest::Proxy::all(proxy)?);
        }
        Ok(Self {
            dialect,
            client: builder.build()?,
            endpoint,
            model: model.into(),
            key_env,
        })
    }

    fn request_body(&self, messages: &[ChatMessage], tools: &[ToolSpec]) -> Value {
        match self.dialect {
            Dialect::Openai => {
                let messages:Vec<Value>=messages.iter().map(|m|match m {
                    ChatMessage::System(s)=>json!({"role":"system","content":s}),
                    ChatMessage::User(s)=>json!({"role":"user","content":s}),
                    ChatMessage::Assistant(s)=>json!({"role":"assistant","content":s}),
                    ChatMessage::AssistantTools(t)=>json!({"role":"assistant","content":t.text,"tool_calls":t.tool_calls.iter().map(|c| json!({"id":c.id,"type":"function","function":{"name":c.name,"arguments":c.arguments.to_string()}})).collect::<Vec<_>>()}),
                    ChatMessage::Tool{call_id,result}=>json!({"role":"tool","tool_call_id":call_id,"content":result.to_string()}),
                }).collect();
                let mut body = json!({"model":self.model,"messages":messages,"stream":true});
                if !tools.is_empty() {
                    body["tools"]=json!(tools.iter().map(|t|json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":t.parameters}})).collect::<Vec<_>>());
                }
                body
            }
            Dialect::Anthropic => {
                let mut system = Vec::new();
                let mut turns: Vec<Value> = Vec::new();
                for msg in messages {
                    let (role, blocks) = match msg {
                        ChatMessage::System(s) => {
                            system.push(s.as_str());
                            continue;
                        }
                        ChatMessage::User(s) => ("user", vec![json!({"type":"text","text":s})]),
                        ChatMessage::Assistant(s) => {
                            ("assistant", vec![json!({"type":"text","text":s})])
                        }
                        ChatMessage::AssistantTools(t) => {
                            let mut blocks = Vec::new();
                            if !t.text.is_empty() {
                                blocks.push(json!({"type":"text","text":t.text}));
                            }
                            blocks.extend(t.tool_calls.iter().map(|c|json!({"type":"tool_use","id":c.id,"name":c.name,"input":c.arguments})));
                            ("assistant", blocks)
                        }
                        ChatMessage::Tool { call_id, result } => (
                            "user",
                            vec![
                                json!({"type":"tool_result","tool_use_id":call_id,"content":result.to_string()}),
                            ],
                        ),
                    };
                    if let Some(last) = turns.last_mut().filter(|v| v["role"] == role) {
                        last["content"]
                            .as_array_mut()
                            .expect("created as array")
                            .extend(blocks);
                    } else {
                        turns.push(json!({"role":role,"content":blocks}));
                    }
                }
                let mut body = json!({"model":self.model,"system":system.join("\n\n"),"messages":turns,"stream":true,"max_tokens":2048});
                if !tools.is_empty() {
                    body["tools"]=json!(tools.iter().map(|t|json!({"name":t.name,"description":t.description,"input_schema":t.parameters})).collect::<Vec<_>>());
                }
                body
            }
        }
    }
}

#[async_trait]
impl BrainBackend for HttpBrain {
    async fn generate_turn(
        &self,
        messages: &[ChatMessage],
        tools: &[ToolSpec],
    ) -> Result<BrainTurnOutput, BrainError> {
        self.generate_turn_with_preview(messages, tools, &|_| {})
            .await
    }
    async fn generate_turn_with_preview(
        &self,
        messages: &[ChatMessage],
        tools: &[ToolSpec],
        preview: &(dyn for<'chunk> Fn(&'chunk str) + Send + Sync),
    ) -> Result<BrainTurnOutput, BrainError> {
        let mut request = self
            .client
            .post(self.endpoint.clone())
            .json(&self.request_body(messages, tools));
        if self.dialect == Dialect::Anthropic {
            request = request.header("anthropic-version", "2023-06-01");
        }
        if let Some(var) = &self.key_env {
            let Some(key) = std::env::var(var).ok().filter(|key| !key.trim().is_empty()) else {
                return Err(BrainError::Unavailable(format!(
                    "missing model credential environment variable {var}"
                )));
            };
            request = match self.dialect {
                Dialect::Openai => request.bearer_auth(key),
                Dialect::Anthropic => request.header("x-api-key", key),
            };
        }
        let mut response = request
            .send()
            .await
            .map_err(|_| BrainError::Unavailable("model connection failed".into()))?;
        let status = response.status();
        if !status.is_success() {
            let message = format!("model returned HTTP {}", status.as_u16());
            return Err(
                if matches!(status.as_u16(), 401 | 403 | 408 | 429) || status.is_server_error() {
                    BrainError::Unavailable(message)
                } else {
                    BrainError::Invalid(message)
                },
            );
        }
        let json_body = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("application/json"));
        let mut pending = Vec::new();
        let mut size = 0;
        let mut stream = StreamTurn::default();
        let mut sent = 0;
        while let Some(bytes) = response
            .chunk()
            .await
            .map_err(|_| BrainError::Unavailable("model stream disconnected".into()))?
        {
            size += bytes.len();
            if size > MAX_RESPONSE {
                return Err(BrainError::Invalid(
                    "model response exceeds byte limit".into(),
                ));
            }
            pending.extend_from_slice(&bytes);
            if !json_body {
                let mut consumed = 0;
                while let Some(end) = pending[consumed..].iter().position(|&c| c == b'\n') {
                    let line = &pending[consumed..consumed + end];
                    stream.line(self.dialect, line)?;
                    consumed += end + 1;
                }
                pending.drain(..consumed);
                let mut n = stream.text.text.len().min(MAX_PREVIEW);
                while !stream.text.text.is_char_boundary(n) {
                    n -= 1
                }
                if !stream.calls.is_empty() {
                    if sent > 0 {
                        preview("");
                        sent = 0
                    }
                } else if n != sent {
                    preview(&stream.text.text[..n]);
                    sent = n;
                }
            }
        }
        if json_body {
            return full_reply(self.dialect, &pending).map_err(invalid);
        }
        stream.line(self.dialect, &pending)?;
        stream.flush(self.dialect)?;
        if !stream.done {
            return Err(BrainError::Unavailable(
                "model stream ended before completion".into(),
            ));
        }
        stream.finish().map_err(invalid)
    }
}

fn invalid(error: anyhow::Error) -> BrainError {
    BrainError::Invalid(error.to_string())
}

#[derive(Default)]
struct PartialCall {
    id: String,
    name: String,
    args: String,
    input: Option<Value>,
}
#[derive(Default)]
struct StreamTurn {
    data: Vec<String>,
    text: crate::visible::VisibleText,
    calls: BTreeMap<u64, PartialCall>,
    done: bool,
    stopped: bool,
}
impl StreamTurn {
    fn line(&mut self, dialect: Dialect, line: &[u8]) -> Result<(), BrainError> {
        let line = std::str::from_utf8(line)
            .map_err(|_| BrainError::Invalid("model returned invalid UTF-8".into()))?
            .trim_end_matches('\r');
        if line.is_empty() {
            return self.flush(dialect);
        }
        if let Some(data) = line.strip_prefix("data:") {
            self.data
                .push(data.strip_prefix(' ').unwrap_or(data).to_owned());
        }
        Ok(())
    }
    fn flush(&mut self, dialect: Dialect) -> Result<(), BrainError> {
        if self.data.is_empty() {
            return Ok(());
        }
        let data = std::mem::take(&mut self.data).join("\n");
        if dialect == Dialect::Openai && data == "[DONE]" {
            self.done = true;
            return Ok(());
        }
        let v: Value = serde_json::from_str(&data)
            .map_err(|_| BrainError::Invalid("malformed SSE event".into()))?;
        if !v["error"].is_null() || v["type"] == "error" {
            return Err(BrainError::Unavailable(
                "upstream model reported a stream error".into(),
            ));
        }
        self.event(dialect, &v).map_err(invalid)
    }
    fn event(&mut self, dialect: Dialect, v: &Value) -> Result<()> {
        match dialect {
            Dialect::Openai => {
                let Some(choice) = v["choices"].as_array().and_then(|choices| choices.first())
                else {
                    return Ok(());
                };
                let delta = &choice["delta"];
                if let Some(text) = delta["content"].as_str() {
                    self.text.append(text);
                }
                for call in delta["tool_calls"].as_array().into_iter().flatten() {
                    let entry = self
                        .calls
                        .entry(call["index"].as_u64().context("missing tool call index")?)
                        .or_default();
                    if let Some(id) = call["id"].as_str() {
                        entry.id.push_str(id);
                    }
                    if let Some(name) = call["function"]["name"].as_str() {
                        entry.name.push_str(name);
                    }
                    if let Some(arg) = call["function"]["arguments"].as_str() {
                        entry.args.push_str(arg);
                    }
                }
                if let Some(reason) = choice["finish_reason"].as_str() {
                    valid_finish(dialect, reason)?;
                    self.stopped = true;
                }
            }
            Dialect::Anthropic => match v["type"].as_str() {
                Some("content_block_start") if v["content_block"]["type"] == "text" => {
                    self.text
                        .append(v["content_block"]["text"].as_str().unwrap_or_default());
                }
                Some("content_block_start") if v["content_block"]["type"] == "tool_use" => {
                    let block = &v["content_block"];
                    self.calls.insert(
                        v["index"].as_u64().context("missing block index")?,
                        PartialCall {
                            id: required_str(block, "id")?.into(),
                            name: required_str(block, "name")?.into(),
                            args: String::new(),
                            input: Some(block["input"].clone()),
                        },
                    );
                }
                Some("content_block_delta") => match v["delta"]["type"].as_str() {
                    Some("text_delta") => self.text.append(required_str(&v["delta"], "text")?),
                    Some("input_json_delta") => {
                        let entry = self
                            .calls
                            .get_mut(&v["index"].as_u64().context("missing block index")?)
                            .context("unknown tool block")?;
                        entry
                            .args
                            .push_str(required_str(&v["delta"], "partial_json")?);
                    }
                    _ => {}
                },
                Some("message_delta") => {
                    if let Some(reason) = v["delta"]["stop_reason"].as_str() {
                        valid_finish(dialect, reason)?;
                        self.stopped = true;
                    }
                }
                Some("message_stop") => self.done = true,
                _ => {}
            },
        }
        Ok(())
    }
    fn finish(self) -> Result<BrainTurnOutput> {
        ensure!(self.stopped, "model omitted completion reason");
        let tool_calls = self
            .calls
            .into_values()
            .map(|c| {
                let arguments = if c.args.is_empty() {
                    c.input.unwrap_or_else(|| json!({}))
                } else {
                    serde_json::from_str(&c.args).context("invalid tool arguments")?
                };
                Ok(ToolCall {
                    id: c.id,
                    name: serde_json::from_value(Value::String(c.name))
                        .context("unknown control tool")?,
                    arguments,
                })
            })
            .collect::<Result<_>>()?;
        Ok(BrainTurnOutput {
            text: self.text.finish(),
            tool_calls,
        })
    }
}

fn required_str<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key]
        .as_str()
        .ok_or_else(|| anyhow!("missing model field {key}"))
}
fn valid_finish(dialect: Dialect, reason: &str) -> Result<()> {
    let ok = match dialect {
        Dialect::Openai => matches!(reason, "stop" | "tool_calls"),
        Dialect::Anthropic => matches!(reason, "end_turn" | "tool_use" | "stop_sequence"),
    };
    ensure!(ok, "model did not complete its decision");
    Ok(())
}
fn full_reply(dialect: Dialect, body: &[u8]) -> Result<BrainTurnOutput> {
    let v: Value = serde_json::from_slice(body).context("invalid model JSON response")?;
    let mut out = BrainTurnOutput::default();
    match dialect {
        Dialect::Openai => {
            let c = &v["choices"][0];
            valid_finish(dialect, required_str(c, "finish_reason")?)?;
            out.text = c["message"]["content"].as_str().unwrap_or_default().into();
            for call in c["message"]["tool_calls"].as_array().into_iter().flatten() {
                let f = &call["function"];
                out.tool_calls.push(ToolCall {
                    id: required_str(call, "id")?.into(),
                    name: serde_json::from_value(f["name"].clone())?,
                    arguments: serde_json::from_str(required_str(f, "arguments")?)?,
                });
            }
        }
        Dialect::Anthropic => {
            valid_finish(dialect, required_str(&v, "stop_reason")?)?;
            for block in v["content"].as_array().context("missing content blocks")? {
                match block["type"].as_str() {
                    Some("text") => out.text.push_str(required_str(block, "text")?),
                    Some("tool_use") => out.tool_calls.push(ToolCall {
                        id: required_str(block, "id")?.into(),
                        name: serde_json::from_value(block["name"].clone())?,
                        arguments: block["input"].clone(),
                    }),
                    Some("thinking" | "redacted_thinking") => {}
                    _ => bail!("unrecognized model response block"),
                }
            }
        }
    }
    out.text = crate::visible::public_string(&out.text);
    Ok(out)
}
