//! Explicitly configured SaaS search projections of *accepted S2 revisions*.
//! S0, dialogue, candidates and credentials are never included in the records.
use super::{cursor, verify_workspace, Memory};
use anyhow::{ensure, Context, Result};
use reqwest::{Client, Method, Url};
use rusqlite::Connection;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

#[derive(Copy, Clone, PartialEq, Eq)]
enum Kind {
    Mem0,
    Honcho,
}

#[derive(Clone)]
pub struct HostedIndex {
    kind: Kind,
    client: Client,
    endpoint: Url,
    api_key: String,
    tenant: String,
    workspace: String,
    published: Arc<Mutex<Option<i64>>>,
}

pub struct Projection {
    cursor: i64,
    original: BTreeMap<String, String>,
    workspace: String,
    current: BTreeMap<String, Memory>,
}
impl Projection {
    pub fn read(db: &mut Connection, workspace: &str) -> Result<Self> {
        let tx = db.transaction()?;
        verify_workspace(&tx, workspace)?;
        let cursor = cursor(&tx)?;
        let mut original = BTreeMap::new();
        let mut current = BTreeMap::new();
        {
            let mut stmt=tx.prepare("SELECT c.revision_json, m.sync_seq = c.sync_seq FROM memory_changes c JOIN memories m ON m.memory_id=c.memory_id")?;
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let item: Memory = serde_json::from_str(&r.get::<_, String>(0)?)?;
                if item.is_deleted {
                    continue;
                }
                // The tag identifies this exact local revision. Managed records are
                // recognized only when both their tag and their content match the log.
                let tag = format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&(
                        &item.workspace_id,
                        &item.memory_id,
                        item.version,
                        &item.content_hash
                    ))?)
                );
                original.insert(tag.clone(), item.content_hash.clone());
                if r.get::<_, bool>(1)? {
                    current.insert(tag, item);
                }
            }
        }
        tx.commit()?;
        Ok(Self {
            cursor,
            original,
            workspace: workspace.into(),
            current,
        })
    }
}

fn text_for(tag: &str, item: &Memory) -> String {
    format!("[Tarla S2 {tag}:{}]\n{}", item.content_hash, item.content)
}
fn tag_of(text: &str) -> Option<(&str, &str)> {
    let (header, body) = text.split_once('\n')?;
    let (tag, digest) = header
        .strip_prefix("[Tarla S2 ")?
        .strip_suffix(']')?
        .split_once(':')?;
    if tag.len() == 64 && digest == format!("{:x}", Sha256::digest(body.as_bytes())) {
        Some((tag, digest))
    } else {
        None
    }
}

impl HostedIndex {
    pub fn new(
        provider: &str,
        workspace: &str,
        namespace: &str,
        endpoint: Option<&str>,
        api_key: String,
    ) -> Result<Self> {
        let (kind, default) = match provider {
            "mem0" => (Kind::Mem0, "https://api.mem0.ai"),
            "honcho" => (Kind::Honcho, "https://api.honcho.dev"),
            _ => anyhow::bail!("unknown hosted memory provider"),
        };
        ensure!(
            !namespace.trim().is_empty() && !api_key.trim().is_empty(),
            "cloud_namespace and provider API key are required"
        );
        let endpoint = Url::parse(endpoint.unwrap_or(default))?;
        let local = endpoint
            .host_str()
            .is_some_and(|h| matches!(h, "127.0.0.1" | "localhost" | "[::1]"));
        ensure!(
            (endpoint.scheme() == "https" || (endpoint.scheme() == "http" && local))
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.path() == "/",
            "memory API origin must be HTTPS or loopback HTTP without credentials, path or query"
        );
        let key = serde_json::to_vec(&(namespace, workspace))?;
        let tenant = format!("tarla-{:x}", Sha256::digest(&key));
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            kind,
            client,
            endpoint,
            api_key,
            tenant: tenant[..38].to_owned(),
            workspace: workspace.into(),
            published: Default::default(),
        })
    }

    async fn request(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let url = self.endpoint.join(path)?;
        let auth = if self.kind == Kind::Mem0 {
            "Token"
        } else {
            "Bearer"
        };
        let mut req = self
            .client
            .request(method.clone(), url)
            .header("Authorization", format!("{auth} {}", self.api_key));
        if let Some(v) = body {
            req = req.json(&v);
        }
        let mut res = req.send().await?;
        if method == Method::DELETE && res.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Value::Null);
        }
        let status = res.status();
        ensure!(
            status.is_success(),
            "hosted memory API returned HTTP {status}"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = res.chunk().await? {
            ensure!(
                bytes.len() + chunk.len() <= 16 * 1024 * 1024,
                "oversized memory provider response"
            );
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(serde_json::from_slice(&bytes)?)
        }
    }
    fn honcho_path(&self, action: &str) -> String {
        format!("/v3/workspaces/{}/{}", self.tenant, action)
    }
    fn filters(&self) -> Value {
        match self.kind {
            Kind::Mem0 => json!({"user_id":self.tenant}),
            Kind::Honcho => json!({"observer_id":"tarla-s2","observed_id":"tarla-s2"}),
        }
    }

    pub async fn is_current(&self, cursor: i64) -> bool {
        self.published
            .lock()
            .await
            .is_some_and(|published| published >= cursor)
    }

    /// Perform reconciliation only on the designated supervisor. An interrupted
    /// write is rediscovered by listing the dedicated provider scope on retry.
    pub async fn publish(&self, p: &Projection) -> Result<()> {
        ensure!(p.workspace == self.workspace, "wrong cloud workspace");
        let mut cursor = self.published.lock().await;
        if cursor.is_some_and(|already| already >= p.cursor) {
            return Ok(());
        }
        if self.kind == Kind::Honcho {
            self.request(
                Method::POST,
                "/v3/workspaces",
                Some(json!({"id":self.tenant})),
            )
            .await?;
            self.request(
                Method::POST,
                &self.honcho_path("peers"),
                Some(json!({"id":"tarla-s2"})),
            )
            .await?;
        }
        let mut live = BTreeSet::new();
        let mut remove = BTreeSet::new();
        let mut page = 1u64;
        loop {
            let (path, field) = match self.kind {
                Kind::Mem0 => (
                    format!("/v3/memories/?page={page}&page_size=100"),
                    "results",
                ),
                Kind::Honcho => (
                    self.honcho_path(&format!("conclusions/list?page={page}&size=100")),
                    "items",
                ),
            };
            let reply = self
                .request(Method::POST, &path, Some(json!({"filters":self.filters()})))
                .await?;
            let items = reply[field]
                .as_array()
                .context("invalid provider list response")?;
            for row in items {
                let Some((tag, digest)) = self.record_text(row).and_then(tag_of) else {
                    continue;
                };
                // Never delete native records, another workspace's records, or
                // future revisions which a stale supervisor has not seen.
                if !p
                    .original
                    .get(tag)
                    .is_some_and(|original| original == digest)
                {
                    continue;
                }
                if !p.current.contains_key(tag) || !live.insert(tag.to_owned()) {
                    let id = row["id"].as_str().context("provider record has no id")?;
                    ensure!(
                        id.len() <= 128
                            && !id.is_empty()
                            && id
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                        "invalid provider record id"
                    );
                    remove.insert(id.to_owned());
                }
            }
            let next = match self.kind {
                Kind::Mem0 => !reply["next"].is_null(),
                Kind::Honcho => {
                    page < reply["pages"]
                        .as_u64()
                        .context("invalid Honcho pagination")?
                }
            };
            if !next {
                break;
            }
            ensure!(!items.is_empty(), "provider pagination made no progress");
            page = page.checked_add(1).context("provider page exhausted")?;
        }
        for (tag, m) in &p.current {
            if live.contains(tag) {
                continue;
            }
            let content = text_for(tag, m);
            match self.kind {
                Kind::Mem0 => {
                    let reply=self.request(Method::POST,"/v3/memories/add/",Some(json!({
                        "user_id":self.tenant,"infer":false,"messages":[{"role":"user","content":content}]
                    }))).await?;
                    ensure!(
                        reply["results"].as_array().is_some_and(|a| !a.is_empty()),
                        "Mem0 did not acknowledge verbatim memory"
                    );
                }
                Kind::Honcho => {
                    let reply = self
                        .request(
                            Method::POST,
                            &self.honcho_path("conclusions"),
                            Some(json!({"conclusions":[{
                                "observer_id":"tarla-s2","observed_id":"tarla-s2","content":content
                            }]})),
                        )
                        .await?;
                    ensure!(
                        reply.as_array().is_some_and(|a| !a.is_empty()),
                        "Honcho did not acknowledge conclusion"
                    );
                }
            }
        }
        for id in remove {
            let path = match self.kind {
                Kind::Mem0 => format!("/v1/memories/{id}/"),
                Kind::Honcho => self.honcho_path(&format!("conclusions/{id}")),
            };
            self.request(Method::DELETE, &path, None).await?;
        }
        *cursor = Some(p.cursor);
        Ok(())
    }
    fn record_text<'r>(&self, row: &'r Value) -> Option<&'r str> {
        row[if self.kind == Kind::Mem0 {
            "memory"
        } else {
            "content"
        }]
        .as_str()
    }

    pub async fn search(&self, p: &Projection, query: &str, limit: usize) -> Result<Vec<Memory>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let path = match self.kind {
            Kind::Mem0 => "/v3/memories/search/".into(),
            Kind::Honcho => self.honcho_path("conclusions/query"),
        };
        let reply = self
            .request(
                Method::POST,
                &path,
                Some(json!({"query":query,"top_k":limit.min(100),"filters":self.filters()})),
            )
            .await?;
        let hits = if self.kind == Kind::Mem0 {
            &reply["results"]
        } else {
            &reply
        };
        let mut seen = BTreeSet::new();
        let mut result = Vec::new();
        for row in hits
            .as_array()
            .context("invalid hosted memory search response")?
        {
            if let Some((tag, digest)) = self.record_text(row).and_then(tag_of) {
                if let Some(m) = p.current.get(tag).filter(|m| {
                    m.workspace_id == self.workspace && m.content_hash == digest && seen.insert(tag)
                }) {
                    result.push(m.clone())
                }
            }
            if result.len() >= limit {
                break;
            }
        }
        Ok(result)
    }
}
