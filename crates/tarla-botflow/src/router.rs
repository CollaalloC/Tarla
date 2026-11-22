//! EventEnvelope bus, 3-policy SQLite `botflow_inbox` scheduler (`coalesce_latest` + `debounce_ms`,
//! `backpressure`, `drop_oldest`), 6-hop circuit breaker, and multi-bot 3-round / 10-reply guard.
use crate::spec::{BotflowSpec, QueuePolicy};
use anyhow::{bail, ensure, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub event_id: String,
    pub rev: i64,
    pub workspace_id: String,
    pub source_topic: String,
    pub correlation_id: String,
    pub hop_count: u8,
    pub hlc_timestamp: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PublishOutcome {
    Queued {
        inbox_id: String,
        target_bot_id: String,
        input_port: String,
        deliver_after_ms: i64,
    },
    Coalesced {
        inbox_id: String,
        target_bot_id: String,
        input_port: String,
        coalesced_count: i64,
        deliver_after_ms: i64,
    },
    DroppedOldest {
        evicted_inbox_id: String,
        inbox_id: String,
        target_bot_id: String,
        input_port: String,
    },
    TrippedHopBreaker {
        inbox_id: String,
        target_bot_id: String,
        input_port: String,
        hop_count: u8,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InboxDelivery {
    pub inbox_id: String,
    pub target_bot_id: String,
    pub input_port: String,
    pub correlation_id: String,
    pub hop_count: u8,
    pub coalesced_count: i64,
    pub payload: Value,
}

/// Publish an event from `source_topic` (`<bot>/<port>` or `sys/<topic>`) into `sync_ledger`
/// and all subscribed `botflow_inbox` ports in a single atomic SQLite `Immediate` transaction.
#[allow(clippy::too_many_arguments)]
pub fn publish_event(
    db: &mut Connection,
    spec: &BotflowSpec,
    source_topic: &str,
    correlation_id: Option<&str>,
    hop_count: u8,
    hlc_timestamp: &str,
    payload: Value,
    now_ms: i64,
) -> Result<(EventEnvelope, Vec<PublishOutcome>)> {
    ensure!(
        source_topic.contains('/'),
        "source_topic `{source_topic}` must be `<bot>/<port>` or `sys/<topic>`"
    );
    let event_id = Uuid::now_v7().to_string();
    let corr = correlation_id
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| event_id.clone());

    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let rev = tarla_memory::record_sync_event(
        &tx,
        &spec.workspace,
        "botflow_event",
        source_topic,
        &json!({
            "event_id": event_id,
            "source_topic": source_topic,
            "correlation_id": corr,
            "hop_count": hop_count,
            "hlc_timestamp": hlc_timestamp,
            "payload": payload,
        }),
        now_ms / 1000,
    )?;

    let envelope = EventEnvelope {
        event_id,
        rev,
        workspace_id: spec.workspace.clone(),
        source_topic: source_topic.to_owned(),
        correlation_id: corr.clone(),
        hop_count,
        hlc_timestamp: hlc_timestamp.to_owned(),
        payload: payload.clone(),
    };
    let payload_str = serde_json::to_string(&envelope)?;
    let mut outcomes = Vec::new();

    for bot in &spec.bots {
        for (port_name, port_cfg) in &bot.inputs {
            if port_cfg.source != source_topic {
                continue;
            }
            if hop_count >= spec.policy.max_event_hops {
                let breaker_id = Uuid::now_v7().to_string();
                tx.execute(
                    "INSERT INTO botflow_inbox(
                        inbox_id, target_bot_id, input_port, correlation_id,
                        hop_count, coalesced_count, deliver_after, payload_json, status
                     ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, 'tripped_breaker')",
                    params![
                        breaker_id,
                        bot.id,
                        port_name,
                        corr,
                        i64::from(hop_count),
                        now_ms,
                        payload_str
                    ],
                )?;
                tarla_memory::record_sync_event(
                    &tx,
                    &spec.workspace,
                    "approval",
                    &breaker_id,
                    &json!({
                        "kind": "hop_circuit_breaker",
                        "target_bot_id": bot.id,
                        "input_port": port_name,
                        "correlation_id": corr,
                        "hop_count": hop_count,
                    }),
                    now_ms / 1000,
                )?;
                outcomes.push(PublishOutcome::TrippedHopBreaker {
                    inbox_id: breaker_id,
                    target_bot_id: bot.id.clone(),
                    input_port: port_name.clone(),
                    hop_count,
                });
                continue;
            }

            match port_cfg.queue_policy {
                QueuePolicy::CoalesceLatest => {
                    let deliver_after = now_ms.saturating_add(port_cfg.debounce_ms as i64);
                    let existing: Option<(String, i64)> = tx
                        .query_row(
                            "SELECT inbox_id, coalesced_count FROM botflow_inbox
                             WHERE target_bot_id = ?1 AND input_port = ?2 AND status = 'queued'
                             ORDER BY rowid DESC LIMIT 1",
                            params![bot.id, port_name],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .optional()?;
                    if let Some((inbox_id, prev_count)) = existing {
                        let new_count = prev_count + 1;
                        tx.execute(
                            "UPDATE botflow_inbox
                             SET correlation_id = ?1,
                                 hop_count = ?2,
                                 coalesced_count = ?3,
                                 deliver_after = ?4,
                                 payload_json = ?5
                             WHERE inbox_id = ?6",
                            params![
                                corr,
                                i64::from(hop_count),
                                new_count,
                                deliver_after,
                                payload_str,
                                inbox_id
                            ],
                        )?;
                        outcomes.push(PublishOutcome::Coalesced {
                            inbox_id,
                            target_bot_id: bot.id.clone(),
                            input_port: port_name.clone(),
                            coalesced_count: new_count,
                            deliver_after_ms: deliver_after,
                        });
                    } else {
                        let inbox_id = Uuid::now_v7().to_string();
                        tx.execute(
                            "INSERT INTO botflow_inbox(
                                inbox_id, target_bot_id, input_port, correlation_id,
                                hop_count, coalesced_count, deliver_after, payload_json, status
                             ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, 'queued')",
                            params![
                                inbox_id,
                                bot.id,
                                port_name,
                                corr,
                                i64::from(hop_count),
                                deliver_after,
                                payload_str
                            ],
                        )?;
                        outcomes.push(PublishOutcome::Queued {
                            inbox_id,
                            target_bot_id: bot.id.clone(),
                            input_port: port_name.clone(),
                            deliver_after_ms: deliver_after,
                        });
                    }
                }
                QueuePolicy::Backpressure => {
                    let inbox_id = Uuid::now_v7().to_string();
                    tx.execute(
                        "INSERT INTO botflow_inbox(
                            inbox_id, target_bot_id, input_port, correlation_id,
                            hop_count, coalesced_count, deliver_after, payload_json, status
                         ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, 'queued')",
                        params![
                            inbox_id,
                            bot.id,
                            port_name,
                            corr,
                            i64::from(hop_count),
                            now_ms,
                            payload_str
                        ],
                    )?;
                    outcomes.push(PublishOutcome::Queued {
                        inbox_id,
                        target_bot_id: bot.id.clone(),
                        input_port: port_name.clone(),
                        deliver_after_ms: now_ms,
                    });
                }
                QueuePolicy::DropOldest => {
                    let cap = spec.policy.drop_oldest_capacity.max(1);
                    let queued_ids: Vec<String> = {
                        let mut s = tx.prepare(
                            "SELECT inbox_id FROM botflow_inbox
                             WHERE target_bot_id = ?1 AND input_port = ?2 AND status = 'queued'
                             ORDER BY rowid ASC",
                        )?;
                        let list = s
                            .query_map(params![bot.id, port_name], |r| r.get(0))?
                            .collect::<rusqlite::Result<Vec<_>>>()?;
                        list
                    };
                    let mut evicted = None;
                    if queued_ids.len() >= cap {
                        let oldest = queued_ids[0].clone();
                        tx.execute(
                            "UPDATE botflow_inbox SET status = 'dropped_oldest' WHERE inbox_id = ?1",
                            [&oldest],
                        )?;
                        evicted = Some(oldest);
                    }
                    let inbox_id = Uuid::now_v7().to_string();
                    tx.execute(
                        "INSERT INTO botflow_inbox(
                            inbox_id, target_bot_id, input_port, correlation_id,
                            hop_count, coalesced_count, deliver_after, payload_json, status
                         ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, 'queued')",
                        params![
                            inbox_id,
                            bot.id,
                            port_name,
                            corr,
                            i64::from(hop_count),
                            now_ms,
                            payload_str
                        ],
                    )?;
                    if let Some(evicted_inbox_id) = evicted {
                        outcomes.push(PublishOutcome::DroppedOldest {
                            evicted_inbox_id,
                            inbox_id,
                            target_bot_id: bot.id.clone(),
                            input_port: port_name.clone(),
                        });
                    } else {
                        outcomes.push(PublishOutcome::Queued {
                            inbox_id,
                            target_bot_id: bot.id.clone(),
                            input_port: port_name.clone(),
                            deliver_after_ms: now_ms,
                        });
                    }
                }
            }
        }
    }

    tx.commit()?;
    Ok((envelope, outcomes))
}

/// Claim the next deliverable inbox item for `target_bot_id`.
/// Strictly withholds delivery when `human_lease_active` (Issue #75 draft protection)
/// or `bot_busy` (preserving `coalesce_latest` merging while a run is active!).
pub fn claim_next_inbox(
    db: &mut Connection,
    target_bot_id: &str,
    now_ms: i64,
    human_lease_active: bool,
    bot_busy: bool,
) -> Result<Option<InboxDelivery>> {
    if human_lease_active || bot_busy {
        return Ok(None);
    }
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<(String, String, String, u8, i64, String)> = tx
        .query_row(
            "SELECT inbox_id, input_port, correlation_id, hop_count, coalesced_count, payload_json
             FROM botflow_inbox
             WHERE target_bot_id = ?1 AND status = 'queued' AND deliver_after <= ?2
             ORDER BY deliver_after ASC, rowid ASC
             LIMIT 1",
            params![target_bot_id, now_ms],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get::<_, i64>(3)? as u8,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((inbox_id, input_port, correlation_id, hop_count, coalesced_count, payload_raw)) = row
    else {
        return Ok(None);
    };
    tx.execute(
        "UPDATE botflow_inbox SET status = 'delivered' WHERE inbox_id = ?1",
        [&inbox_id],
    )?;
    tx.commit()?;
    let payload: Value = serde_json::from_str(&payload_raw)?;
    Ok(Some(InboxDelivery {
        inbox_id,
        target_bot_id: target_bot_id.to_owned(),
        input_port,
        correlation_id,
        hop_count,
        coalesced_count,
        payload,
    }))
}

/// Extract explicit `@bot_id` mentions from collaborative room messages.
pub fn extract_bot_mentions(text: &str, declared_bots: &[String]) -> Vec<String> {
    let mut mentioned = Vec::new();
    for token in text.split(|c: char| {
        c.is_whitespace() || matches!(c, ',' | ';' | ':' | '!' | '?' | '(' | ')' | '，' | '。')
    }) {
        if let Some(handle) = token.strip_prefix('@') {
            if declared_bots.iter().any(|b| b == handle) && !mentioned.iter().any(|m| m == handle) {
                mentioned.push(handle.to_owned());
            }
        }
    }
    mentioned
}

/// Enforce Spec §4.4 multi-bot room collaboration anti-loop hard limits:
/// max `3` rounds, max `10` total Bot replies per human turn, and from round >= 2
/// only wake Bots explicitly `@bot_id` mentioned.
pub fn admit_room_round(
    db: &mut Connection,
    spec: &BotflowSpec,
    correlation_id: &str,
    round_index: u32,
    message_text: &str,
    candidate_bots: &[String],
    now_secs: i64,
) -> Result<Vec<String>> {
    ensure!(round_index >= 1, "round_index is 1-based");
    if round_index > spec.policy.max_room_rounds {
        bail!(
            "room circuit breaker tripped: round {} exceeds max_room_rounds ({})",
            round_index,
            spec.policy.max_room_rounds
        );
    }
    let declared: Vec<String> = spec.bots.iter().map(|b| b.id.clone()).collect();
    let targets = if round_index >= 2 {
        extract_bot_mentions(message_text, &declared)
            .into_iter()
            .filter(|b| candidate_bots.contains(b))
            .collect::<Vec<_>>()
    } else {
        candidate_bots.to_vec()
    };
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (_, prev_replies): (u32, u32) = tx
        .query_row(
            "SELECT round_count, reply_count FROM botflow_room_turns WHERE correlation_id = ?1",
            [correlation_id],
            |r| Ok((r.get::<_, i64>(0)? as u32, r.get::<_, i64>(1)? as u32)),
        )
        .optional()?
        .unwrap_or((0, 0));

    let remaining = spec.policy.max_room_replies.saturating_sub(prev_replies) as usize;
    if remaining == 0 {
        bail!(
            "room circuit breaker tripped: cumulative bot replies reached max_room_replies ({})",
            spec.policy.max_room_replies
        );
    }
    let admitted: Vec<String> = targets.into_iter().take(remaining).collect();
    let new_replies = prev_replies + admitted.len() as u32;
    tx.execute(
        "INSERT INTO botflow_room_turns(correlation_id, workspace_id, round_count, reply_count, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(correlation_id) DO UPDATE SET
            round_count = MAX(round_count, excluded.round_count),
            reply_count = excluded.reply_count,
            updated_at = excluded.updated_at",
        params![
            correlation_id,
            spec.workspace,
            i64::from(round_index),
            i64::from(new_replies),
            now_secs
        ],
    )?;
    tx.commit()?;
    Ok(admitted)
}
