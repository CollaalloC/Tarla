use anyhow::Result;
use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tarla_memory::{HostedIndex, MemoryBackend, Mutation, Projection, SqliteMemory};
use tokio::sync::Mutex;

struct Fixture {
    provider: String,
    records: BTreeMap<String, String>,
    next_id: u32,
    fail_add_once: bool,
    tenant: Option<String>,
}
type Shared = Arc<Mutex<Fixture>>;

async fn provider(State(state): State<Shared>, req: Request<Body>) -> Response {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    let auth = req
        .headers()
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let raw = to_bytes(req.into_body(), 100_000).await.unwrap();
    let body: Value = serde_json::from_slice(&raw).unwrap_or(Value::Null);
    let mut svc = state.lock().await;
    let mem0 = svc.provider == "mem0";
    assert_eq!(
        auth,
        if mem0 {
            "Token test-key"
        } else {
            "Bearer test-key"
        }
    );
    let rows = svc
        .records
        .iter()
        .map(|(id, text)| {
            if mem0 {
                json!({"id":id,"memory":text})
            } else {
                json!({"id":id,"content":text})
            }
        })
        .collect::<Vec<_>>();
    let response = if method == "DELETE" {
        let key = path.trim_end_matches('/').rsplit('/').next().unwrap();
        assert!(svc.records.remove(key).is_some());
        json!({})
    } else if path == "/v3/workspaces" {
        svc.tenant = Some(body["id"].as_str().unwrap().into());
        json!({"id":body["id"]})
    } else if path.ends_with("/peers") {
        assert_eq!(body["id"], "tarla-s2");
        json!({"id":"tarla-s2"})
    } else if path == "/v3/memories/add/" || path.ends_with("/conclusions") {
        let content = if mem0 {
            assert_eq!(body["infer"], false);
            assert_eq!(body["user_id"].as_str(), svc.tenant.as_deref());
            body["messages"][0]["content"].as_str().unwrap()
        } else {
            assert_eq!(body["conclusions"][0]["observer_id"], "tarla-s2");
            body["conclusions"][0]["content"].as_str().unwrap()
        };
        svc.next_id += 1;
        let id = format!("r{}", svc.next_id);
        svc.records.insert(id.clone(), content.into());
        if svc.fail_add_once {
            svc.fail_add_once = false;
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        if mem0 {
            json!({"results":[{"id":id}]})
        } else {
            json!([{"id":id}])
        }
    } else if path == "/v3/memories/search/" || path.ends_with("/conclusions/query") {
        assert_eq!(body["query"], "preference");
        if mem0 {
            json!({"results":rows})
        } else {
            json!(rows)
        }
    } else if path == "/v3/memories/" || path.ends_with("/conclusions/list") {
        if mem0 {
            let tenant = body["filters"]["user_id"].as_str().unwrap();
            assert!(tenant.starts_with("tarla-"));
            svc.tenant = Some(tenant.into());
            json!({"next":null,"results":rows})
        } else {
            assert!(path.contains(svc.tenant.as_ref().unwrap()));
            assert_eq!(body["filters"]["observed_id"], "tarla-s2");
            json!({"pages":1,"items":rows})
        }
    } else {
        panic!("unexpected endpoint {path}")
    };
    Json(response).into_response()
}

#[tokio::test]
async fn hosted_provider_contracts_recover_from_ambiguous_writes() -> Result<()> {
    for kind in ["mem0", "honcho"] {
        let fixture = Arc::new(Mutex::new(Fixture {
            provider: kind.into(),
            records: BTreeMap::from([("native".into(), "User-authored document".into())]),
            next_id: 0,
            fail_add_once: true,
            tenant: None,
        }));
        let app = Router::new().fallback(provider).with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let running = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let index = HostedIndex::new(
            kind,
            "work",
            "this installation",
            Some(&origin),
            "test-key".into(),
        )?;
        let mut db = tarla_memory::open(Path::new(":memory:"), "work")?;
        let mut p = Mutation {
            proposal_id: "p1".into(),
            memory_id: "settings".into(),
            scope: "global".into(),
            content: "Prefer dark theme".into(),
            base_version: 0,
            source_node_id: "supervisor".into(),
            source_run_id: None,
            is_deleted: false,
            hlc_timestamp: None,
        };
        SqliteMemory::new(&mut db, "work", "supervisor").propose_mutation(&p)?;
        let snapshot = Projection::read(&mut db, "work")?;
        assert!(index.publish(&snapshot).await.is_err());
        index.publish(&snapshot).await?;
        assert_eq!(fixture.lock().await.records.len(), 2);
        assert_eq!(
            index.search(&snapshot, "preference", 10).await?[0].content,
            p.content
        );
        p.proposal_id = "p2".into();
        p.content = "Prefer light theme".into();
        p.base_version = 1;
        SqliteMemory::new(&mut db, "work", "supervisor").propose_mutation(&p)?;
        let updated = Projection::read(&mut db, "work")?;
        index.publish(&updated).await?;
        assert_eq!(fixture.lock().await.records.len(), 2);
        assert_eq!(
            index.search(&updated, "preference", 10).await?[0].version,
            2
        );
        p.proposal_id = "p3".into();
        p.is_deleted = true;
        p.base_version = 2;
        SqliteMemory::new(&mut db, "work", "supervisor").propose_mutation(&p)?;
        let archived = Projection::read(&mut db, "work")?;
        index.publish(&archived).await?;
        assert_eq!(
            fixture
                .lock()
                .await
                .records
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["native"]
        );
        assert!(index.search(&archived, "preference", 10).await?.is_empty());
        running.abort();
    }
    Ok(())
}
