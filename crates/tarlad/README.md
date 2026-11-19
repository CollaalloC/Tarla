# Headless workspace service

Start one service per initialized workspace with `tarlad --home ~/.tarla --workspace work --listen 127.0.0.1:7878`. The service holds the workspace lock for its lifetime. The client uses `runtime/api.token`. Each paired core uses a separate pair-specific credential declared in `[[peers]]`. Duplicate source IDs for the same conversation and text return the original assistant reply.

## Run lifecycle

The supervisor commits an immutable run ID and target. The executor commits a unique claim before asking its private Herdr holder to launch the configured argv. An ambiguous launch reconciles using the run's permanent label. If the executor cannot establish whether a process started, it keeps that run unconfirmed. Confirmed completions and pending human requests enter a durable outbox. The supervisor acknowledges events after committing their effects and the completion wake. An executor reconnects to poll assignments and replay unacknowledged events. The supervisor's term and workspace ID must match the configured epoch on both ends.

For a slot of type `local`, the last argv entry is a JSON mission. The executable is selected by the operator.

```toml
[[slots]]
slot_id = "project-worker"
driver = "local"
model_family = "local-worker"
billing_tier = "local_zero_cost"
supported_roles = ["implementer"]
argv = ["/opt/agents/project-worker"]
workdir = "/srv/projects/example"

[[facts]]
fact_key = "project"
local_path = "/srv/projects/example"
metadata = { project = "example" }

[[peers]]
node_id = "office-mini"
url = "https://office-mini.example"
token_env = "TARLA_WORK_OFFICE_PAIR"
```

The supervisor may require `project` in a mission's `required_fact_keys`. The executor checks that its configured path exists before launching. Other nodes receive fact keys and the configured metadata. The owner retains its filesystem path.

## Executor callbacks

On each launch the service sets `TARLA_RUN_ID`, `TARLA_HOOK_URL` and `TARLA_RUN_TOKEN` in the private terminal's process environment. Send JSON by HTTP POST to the hook URL plus the operation, using the run token in the Authorization bearer header. Callbacks apply to the active run only. A worker that uses a PreToolUse hook must wait for an explicit decision before performing the gated action.

| Operation | Example request | Response or effect |
| --- | --- | --- |
| `/approval` | `{"request_id":"tool-17","tool_or_cmd":"change project settings","risk_reason":"This changes the build configuration."}` | Returns `approval_id` and `resolution`. Repost the **identical** payload to poll. Proceed only on `approved`. Rejection or a lost connection leaves permission withheld. |
| `/memory` | `{"proposal_id":"fixture-rule","memory_id":"fixture","base_version":0,"scope":"project:lab","content":"The fixture is measured at 285 mm."}` | Commits a local proposal before returning. The supervisor assigns its new version or queues a conflict for review. |
| `/receipt` | `{"executive_summary":"Completed the calibration check.","artifact_refs":["results/report.csv"]}` | Stores a bounded receipt draft. The executor sets the final state when it observes the process exit. |
| `/telemetry` | `{"health":"healthy","window_used_pct":12,"weekly_used_pct":35,"window_resets_at":1800000000}` | Updates this run's slot. Other health variants include `{"auth_expired":{"message":"Sign in again"}}` and `{"cooling_down":{"resets_at_unix":1800000000}}`. |

An executor also accepts manual `POST /v1/memories` while its supervisor is offline. Supply a stable `proposal_id` on retries. The response is `queued` until the supervisor applies it. Check `GET /v1/memories` after sync, or `GET /v1/actions` on the supervisor for version conflicts.

Use `GET /v1/runs/{id}/log?offset=0&limit=40` for paged S0 capture. Offsets count journal records. Advance with `next_offset` and inspect `eof`. Optional `query` filters returned text without copying the complete transcript. A capture gap and a final screen snapshot are recorded separately from continuously observed bytes.

## Managed mesh

In `[network]` use `mode="managed_sidecar"` and enable EasyTier, Tailscale, or both. For EasyTier set `easytier_network_name`, `easytier_network_secret_ref="env:WORK_MESH_SECRET"`, and `easytier_peers=["tcp://seed.example:11010"]` as appropriate. A `keychain:` secret reference selects the supported host credential store. Managed userspace ingress forwards the daemon's bound API port to loopback. Peer URLs must use that remote mesh address and port. On Tailscale, authorize the managed instance in its own tailnet and restrict incoming traffic with that tailnet's ACLs. The `preferred="easytier"` or `preferred="tailscale"` setting selects the outbound peer dialer when both are enabled. A named model backend may set `socks_proxy="managed:easytier"` or `"managed:tailscale"`. A failed managed transport does not switch the workspace to the host route.

## Pinned turns and handoff

Use `tarla bind` to select a workspace conversation, executor, configured slot and its exact workdir. Messages sent through any paired channel use that binding. `/switch worker/slot-name` selects an eligible destination. `/switch` selects the best eligible alternative. `@slot-name follow-up` switches and sends one bounded follow-up. The generation change, structured checkpoint and duplicate-command receipt are transactional. A running, blocked or unconfirmed turn must finish before switching. A pinned slot whose quota or health becomes ineligible is reconsidered on the next turn.

Codex slots should configure `argv=["/path/to/codex","exec"]`, optionally followed by normal exec options. Claude slots should configure `argv=["/path/to/claude"]`. Tarla supplies structured output and resume flags on pinned turns. It captures the native UUID before terminal line wrapping and reuses it inside the same slot profile, workdir and binding generation. A different slot receives the previous bounded task objectives, receipts and artifact references. Other drivers use those structured checkpoints on each turn. Billing, model and sandbox configuration stay with the slot. Protocol references: [Codex non-interactive mode](https://developers.openai.com/codex/noninteractive/), [Claude CLI](https://code.claude.com/docs/en/cli-reference).

A local usage monitor may obtain a one-hour credential with bearer-authenticated `POST /v1/slots/{slot}/telemetry-token`. Use the returned credential for `POST /telemetry/v1/slots/{slot}` on that executor. Partial reports support `window_used_pct`, `weekly_used_pct`, both reset timestamps and `health`. Include an optional Unix-seconds `sampled_at` to discard out-of-order readings. The supervisor learns the updated slot through authenticated peer sync.

## Workspace skills in a conversation

Install a UTF-8 instruction file at `workspaces/<workspace>/skills/<name>/SKILL.md`. Send `/skills` in any conversation to list the names available there. Send `/skill calibrate check the fixture` to apply the complete instructions from `skills/calibrate/SKILL.md` to that task.

The supervisor adds the selected revision after the current conversation window for that turn. The frozen system prefix stays stable. A delegated or pinned run receives the selected instructions in its bounded mission packet. Retrying the same source message uses the original revision. A subsequent request can select a newly edited revision. An oversized activation produces an error before dispatch so the executor never receives partial instructions.
