# TriForge · 三元锻造

> **TriForge · 三元锻造** — an A → B → A multi-agent pipeline
> (Architect designs → Coder implements → Architect reviews) wrapped in a
> FastAPI kanban dashboard and exposed as an MCP server, so Hermes /
> Telegram can drive it from a phone.

The name "TriForge" (三元锻造) reflects the design: three roles
(architect / coder / architect) **forging** a complete system out of a
natural-language requirement.

## What's in here

| Layer | Files | What it does |
|---|---|---|
| **Pipeline** | `triforge_server/agent.py` | LLM-backed agent loop with `read_file` / `write_file` / `finish` tools, approval-yielding generator |
| **Workflow** | `triforge_server/workflow.py` | A→B→A pipeline with approval gates between phases |
| **Events** | `triforge_server/events.py` | In-process pub-sub `EventBus` + `BoardEvent` dataclass |
| **Store** | `triforge_server/store.py` + `persistence.py` | SQLite-backed event + run snapshot store (auto-recovery on restart) |
| **Board API** | `triforge_server/board.py` | FastAPI router: `/board/runs`, `/board/runs/{id}`, `/approve`, `/events` (SSE), `/files` |
| **Dashboard** | `triforge_server/static/index.html` | Single-file dark dashboard, vanilla JS, served at `/` |
| **MCP** | `triforge_server/mcp_server.py` | MCP server exposing `triforge_start` / `triforge_approve` / `triforge_status` for Hermes |
| **Notifiers** | `triforge_server/notifier.py` | Multi-channel push: Feishu / DingTalk / WeChat Work / Telegram / Personal WeChat (iLink Bot API) |
| **iLink** | `triforge_server/wechat_bot.py` | Direct iLink Bot API client for the personal-WeChat channel |
| **Server** | `triforge_server/server.py` | FastAPI app, mounts board router + dashboard, restores runs from DB on startup |

## Quick start

### Prerequisites

- Python 3.11+
- API keys for at least one LLM provider (MiniMax + DeepSeek for the
  default role layout):
  - `MINIMAX_CN_API_KEY` — Architect (design + review)
  - `DEEPSEEK_API_KEY` — Coder (implement)

### Install

```bash
# Either of these works on Windows / macOS / Linux:
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS / Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

The app boots with zero config — `data/settings.json` is created with
sensible defaults on first run; you can fill in API keys from the
in-app **Settings** page (preferred, never edit by hand).

### Start the dashboard + API

**Windows** (one terminal):
```cmd
start.bat
```

**macOS / Linux** (any shell):
```bash
./start.sh
# Or directly:
.venv/bin/python -m uvicorn triforge_server.server:app \
    --host 127.0.0.1 --port 8000
```

Then open <http://127.0.0.1:8000/>.

### Start the MCP server (for Hermes / Telegram)

```bash
./run_mcp_server.sh        # macOS / Linux
run_mcp_server.bat        # Windows
```

Hermes discovers this via `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  - name: triforge
    type: stdio
    command: /root/triforge/run_mcp_server.sh
    enabled: true
```

## Configuration

### Environment variables

All knobs are env-var driven; sensible defaults live in
`triforge_server/config.py` and the in-app **Settings** page (which is
the preferred way to adjust them).

| Var | Default | Effect |
|---|---|---|
| `TRIFORGE_WORKSPACE` | `./workspace` | where `design/`, `src/`, `tests/` live |
| `TRIFORGE_DB_PATH`   | `./data/board.db` | SQLite database location |
| `TRIFORGE_MINIMAX_BASE_URL` | `https://api.minimaxi.com/v1` | Architect LLM endpoint |
| `TRIFORGE_DEEPSEEK_BASE_URL` | `https://api.deepseek.com/v1` | Coder LLM endpoint |
| `TRIFORGE_ARCHITECT_MODEL` | `MiniMax-Text-01` | Architect model name |
| `TRIFORGE_CODER_MODEL` | `deepseek-chat` | Coder model name |
| `TRIFORGE_ILINK_BASE_URL` | `https://ilinkai.weixin.qq.com` | Tencent iLink Bot API (overridable for staging/mocks) |
| `PORT` | (no change; set by `start.sh`/`start.bat` default 8000) | Server bind port |
| `TRIFORGE_HOST` | `127.0.0.1` | Server bind host |
| `TRIFORGE_VENV` | `<project>/.venv` | Path to venv used by `start.*` / `run_mcp_server.*` |
| `MINIMAX_CN_API_KEY` | (required) | API key for Architect |
| `DEEPSEEK_API_KEY` | (required) | API key for Coder |

> The fallback chain for backwards compatibility: `TRIFORGE_DEEPSEEK_BASE_URL`
> first, then the legacy `DEEPSEEK_BASE_URL`. Same for the others. New
> deployments should use the `TRIFORGE_*` names; the old names still work.

### Provider API keys

Enter them in **Settings → Providers** in the UI — they get stored
in `data/settings.json` (gitignored, never committed).

### Notification channels

Open **Settings → Notifications** in the UI to add Feishu / DingTalk
/ WeChat Work / Telegram / Personal WeChat channels. Personal WeChat
goes through Tencent's iLink Bot API — click "Connect Personal WeChat"
and scan the QR that TriForge renders.

## API surface

### Board endpoints

| Method | Path | Notes |
|---|---|---|
| `GET`  | `/board/runs` | kanban list (active + recent), sorted by activity |
| `GET`  | `/board/runs/{id}` | run detail + workspace files + cost estimate |
| `POST` | `/board/runs` | create a new run from a requirement string |
| `POST` | `/board/runs/{id}/approve` | approve / reject / modify (or restart if "interrupted") |
| `POST` | `/board/runs/{id}/cancel` | soft cancel (can be resumed via Continue) |
| `POST` | `/board/runs/{id}/force-stop` | hard stop (status → failed; can be resumed) |
| `POST` | `/board/runs/{id}/resume` | continue from current phase (skips completed ones) |
| `DELETE` | `/board/runs/{id}` | delete a terminal-state run |
| `GET`  | `/board/runs/{id}/files` | workspace file tree |
| `GET`  | `/board/runs/{id}/files/{path}` | read file (path-traversal protected) |
| `GET`  | `/board/runs/{id}/events` | **SSE**: replay historical events, then live stream |

### Notification endpoints

| Method | Path | Notes |
|---|---|---|
| `GET`  | `/board/notifications/platforms` | list supported platforms |
| `GET`  | `/board/notifications/history` | last 200 deliveries (success / failure) |
| `POST` | `/board/notifications/test` | send a test message to a channel |
| `POST` | `/board/notifications/personal-wechat/pair-start` | start a personal-WeChat pair (returns iLink QR image) |
| `GET`  | `/board/notifications/personal-wechat/pair-status?code=…` | long-poll until iLink confirms scan |
| `POST` | `/board/notifications/personal-wechat/pair-cancel` | abort a pending pair |

### Legacy workflow endpoints (unchanged, used by MCP server)

| Method | Path |
|---|---|
| `GET`  | `/health` |
| `POST` | `/workflow/start` |
| `GET`  | `/workflow/{id}/status` |
| `POST` | `/workflow/{id}/approve` |

## Pipeline overview

```
USER (board UI / Telegram / curl)
   │  POST /board/runs  {requirement: "..."}
   ▼
[architect_design]   (MiniMax)
   │  yield ToolCallEvent(write_file design/architecture.md)
   │  ⏸  Awaiting Approval
   ▼ USER approves
[architect_design]   finishes → run_pipeline advances
[architect_design]   finishes phase
   │
   ▼
[coder_implement]    (DeepSeek)
   │  yield ToolCallEvent(write_file src/*.py)
   │  ⏸  Awaiting Approval
   ▼ USER approves
[coder_implement]    finishes → ...
   │
   ▼
[architect_review]   (MiniMax)
   │  yield ToolCallEvent(write_file design/review_report.md)
   │  ⏸  Awaiting Approval
   ▼ USER approves
[architect_review]   finishes → run.status = completed
```

## Persistence + crash recovery

Runs and events are persisted to **SQLite** at `data/board.db`. On
server start, all persisted runs are restored to the in-memory engine.
If the server crashed while a run was awaiting approval, that run is
marked **"interrupted"** — re-approving it restarts the pipeline from
the current phase. If it hit `max_steps` or was force-stopped, it's
marked **"failed"**; the **Continue** button skips any phases already
recorded in `run.completed_phases`.

```bash
sqlite3 data/board.db "SELECT run_id, status, phase, completed_phases FROM runs"
sqlite3 data/board.db "SELECT run_id, kind, ts FROM events ORDER BY ts"
```

## Keyboard shortcuts (dashboard)

| Shortcut | Action |
|---|---|
| `Ctrl/Cmd + N` | Open new task modal |
| `↑` / `↓` | Switch between active runs |
| `A` / `R` / `M` | Approve / Reject / Modify (when a run is awaiting) |
| `Esc` | Close modal |

## Tests

```bash
# All three e2e tests, fresh DB + workspace each time
PYTHONPATH=. .venv/bin/python -c "
import sys; sys.path.insert(0, 'triforge_tests')
from test_pipeline_e2e import main; main()
"
.venv/bin/python triforge_tests/test_board_e2e.py
.venv/bin/python triforge_tests/test_persistence_e2e.py
```

| Test | Verifies |
|---|---|
| `test_pipeline_e2e.py` | Original workflow A→B→A flow with 3 approvals → 3 files written |
| `test_board_e2e.py` | Board API + SSE event stream: 3 phases, 3 approvals, 20 events, path-traversal blocked |
| `test_persistence_e2e.py` | Server crash mid-flight → DB has state → restart → "interrupted" → re-approve → completed |

## Portability

TriForge is intentionally portable:

- **Path handling**: every file path goes through `pathlib.Path`; no
  `os.path.join` shenanigans, no Windows-only separators in code.
- **Encoding**: every I/O call passes `encoding="utf-8"` and `errors="ignore"`
  where appropriate. Set `PYTHONUTF8=1` and `PYTHONIOENCODING=utf-8` env
  vars (the start scripts do this for you).
- **No native deps**: pure-Python wheels only. No compiled extensions
  to break on Alpine / musl / macOS-arm64.
- **Python 3.11+** is the only runtime constraint.
- **Scripts** (Windows `.bat` + Unix `.sh`) cover the same operations;
  both honour `TRIFORGE_VENV` to point at a non-default venv, and
  `PORT` to bind elsewhere.
- **Settings UI is the source of truth** for any user-tunable value
  (API keys, model names, max_steps, notification channels, etc.). Don't
  edit `data/settings.json` by hand.

## Architecture decisions

- **Thin wrapper, not full clone.** We don't pull in `browser-use`,
  Docker, or any other heavy deps. The pipeline is hard-coded A→B→A.
- **SQLite for persistence.** Single-file DB, no external service. WAL
  mode for concurrent reads.
- **In-process EventBus.** Pub-sub lives in `events.py`. If we ever
  need cross-process events, swap the implementation there; workflow.py
  doesn't care.
- **Server binds to 127.0.0.1 only.** No external exposure by default.
  For remote access, tunnel through your existing Caddy /
  cloudflared / Tailscale / etc.
- **Mock-first testing.** `triforge_tests/mock_llm_server.py` is a tiny
  OpenAI-compatible stub used by all 3 e2e tests — no real API keys
  needed in CI.
- **Personal WeChat via iLink Bot API directly.** TriForge holds the
  bot_token itself; there's no bridge daemon the user has to install.
  Pair by scanning a QR in the UI once; thereafter it's `POST
  /ilink/bot/sendmessage` and you're done.

## License

This is a personal project; use as you see fit.