# Workspace memory providers

S2 revisions, pending conflicts, tombstones and the replica cursor live in the local workspace's SQLite database. The supervisor assigns versions. A replica can read its complete mirror offline. Select the search provider under `[memory]` in `workspace.toml`.

| Provider | Where search runs | Setup |
| --- | --- | --- |
| `builtin_sqlite` (default) | SQLite FTS5 with a substring fallback for CJK | Included in `tarlad` |
| `mnemosyne` | Workspace-local Mnemosyne search projection | Install `mnemosyne-memory` in a dedicated Python environment and configure its interpreter |
| `mem0` | Mem0 v3 search, scoped to a user ID derived from the deployment namespace and workspace | Configure a Mem0 API credential on the supervisor |
| `honcho` | Honcho v3 conclusion search in a dedicated workspace and peer pair | Configure a Honcho API credential on the supervisor |

## Local Mnemosyne

Toml example for Linux or macOS. On Windows use the virtual environment's `Scripts/python.exe` path.

```toml
[memory]
primary_backend = "mnemosyne"
mnemosyne_python = "/absolute/path/to/venv/bin/python"
mnemosyne_db_path = "mnemosyne.db"
prefetch_token_budget = 1200
```

Install the optional provider into this environment with `python -m pip install mnemosyne-memory`. Tarla starts it with isolated Python imports, a private provider config directory and offline model-hub mode. On the first nonempty search after a revision, the adapter builds a fresh index from current S2 records and publishes it using the SQLite backup API. Choose an unused database path inside the workspace. Existing unmanaged databases are rejected. The SDK's admission filters can exclude individual entries from its search index. The complete S2 mirror and frozen snapshots remain available in Tarla.

The integration test checks insertion, repeat lookup, replacement and archival against the real SDK. Set `TARLA_MNEMOSYNE_PYTHON` to the interpreter and run `cargo test -p tarla-memory real_mnemosyne -- --ignored`.

## Hosted Mem0 and Honcho

```toml
[memory]
primary_backend = "mem0"
cloud_namespace = "my-personal-installation"
api_key_env = "MEM0_API_KEY"
# endpoint = "https://api.mem0.ai"
```

For Honcho use `primary_backend = "honcho"` and `api_key_env = "HONCHO_API_KEY"`. The default origin is `https://api.honcho.dev`. A custom API origin can use HTTPS or loopback HTTP for a local service. Set the named credential in the supervisor process environment. Use a distinct namespace for each independent installation sharing a cloud account.

Enabling a hosted provider sends current accepted S2 text to that service and forwards nonempty search queries from the supervisor. Transcripts, private task logs and unreviewed proposals stay local. The supervisor periodically reconciles its S2 mirror against provider records. It creates the current revisions and removes its superseded or archived records. Replaying an interrupted publication lists the remote scope first. The adapters recognize only records corresponding to revisions in the workspace's own log. Executors use their local SQLite replica. A provider outage defers publication and supervisor search falls back to SQLite.

The local HTTP contract test exercises both APIs, recovery after ambiguous writes, replacement, filtering and deletion. Hosted account credentials are required for a live service test.

[Mnemosyne SDK](https://github.com/mnemosyne-oss/mnemosyne) · [Mem0 API](https://docs.mem0.ai/api-reference/memory/add-memories) · [Honcho API](https://honcho.dev/docs/v3/api-reference/endpoint/conclusions/create-conclusions)
