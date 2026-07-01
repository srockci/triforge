# OpenManus + Hermes Telegram Bot

> Thin-wrapper implementation of an A→B→A agent pipeline (Architect → Coder → Architect review),
> exposed both as a FastAPI dashboard (board + SSE event stream) and as an MCP server
> so Hermes / Telegram can drive it from your phone.

## What's in here

| Layer | Files | What it does |
|---|---|---|
| **Pipeline** | `openmanus_server/agent.py` | LLM-backed agent loop with read_file / write_file / finish tools, approval-yielding generator |
| **Workflow** | `openmanus_server/workflow.py` | A→B→A pipeline with approval gates between phases |
| **Events** | `openmanus_server/events.py` | In-process pub-sub `EventBus` + `BoardEvent` dataclass |
| **Store** | `openmanus_server/store.py` + `persistence.py` | SQLite-backed event + run snapshot store (auto-recovery on restart) |
| **Board API** | `openmanus_server/board.py` | FastAPI router: `/board/runs`, `/board/runs/{id}`, `/approve`, `/events` (SSE), `/files` |
| **Dashboard** | `openmanus_server/static/index.html` | Single-file Linear-style dark dashboard, vanilla JS, served at `/` |
| **MCP** | `openmanus_server/mcp_server.py` | MCP server exposing `openmanus_start` / `openmanus_approve` / `openmanus_status` for Hermes |
| **Server** | `openmanus_server/server.py` | FastAPI app, mounts board router + dashboard, restores runs from DB on startup |

## Running

### Prerequisites

- Python 3.11+
- API keys for at least one LLM provider:
  - `MINIMAX_CN_API_KEY` (Architect — design + review)
  - `DEEPSEEK_API_KEY` (Coder — implementation)
- (Optional) `OPENMANUS_WORKSPACE` (default: `./workspace`)

### Install

```bash
cd /root/openmanus-integration
python -m venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt   # fastapi, uvicorn, openai, pydantic, mcp, etc.
```

### Start the dashboard + API

```bash
./start.sh
# or:
.venv/bin/python -m uvicorn openmanus_server.server:app \
    --host 127.0.0.1 --port 8000
```

Then visit <http://127.0.0.1:8000/>.

### Start the MCP server (for Hermes / Telegram)

```bash
./run_mcp_server.sh
```

Hermes discovers this via `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  - name: openmanus
    type: stdio
    command: /root/openmanus-integration/run_mcp_server.sh
    enabled: true
```

## API surface

### Board endpoints

| Method | Path | Notes |
|---|---|---|
| `GET` | `/board/runs` | kanban list (active + recent), sorted by activity |
| `GET` | `/board/runs/{id}` | run detail + workspace files + cost estimate |
| `POST` | `/board/runs` | create a new run from a requirement string |
| `POST` | `/board/runs/{id}/approve` | approve / reject / modify (or restart if "interrupted") |
| `GET` | `/board/runs/{id}/files` | workspace file tree |
| `GET` | `/board/runs/{id}/files/{path}` | read file (path-traversal protected) |
| `GET` | `/board/runs/{id}/events` | **SSE**: replay historical events, then live stream |

### Legacy workflow endpoints (unchanged, used by MCP server)

| Method | Path |
|---|---|
| `GET` | `/health` |
| `POST` | `/workflow/start` |
| `GET` | `/workflow/{id}/status` |
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

Runs and events are persisted to **SQLite** at `data/board.db`. On server start,
all persisted runs are restored to the in-memory engine. If the server crashed
while a run was awaiting approval, that run is marked **"interrupted"** —
re-approving it restarts the pipeline from the current phase.

```bash
sqlite3 data/board.db "SELECT run_id, status, phase FROM runs"
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
import sys; sys.path.insert(0, 'openmanus_tests')
from test_pipeline_e2e import main; main()
"
.venv/bin/python openmanus_tests/test_board_e2e.py
.venv/bin/python openmanus_tests/test_persistence_e2e.py
```

| Test | Verifies |
|---|---|
| `test_pipeline_e2e.py` | Original workflow A→B→A flow with 3 approvals → 3 files written |
| `test_board_e2e.py` | Board API + SSE event stream: 3 phases, 3 approvals, 20 events, path-traversal blocked |
| `test_persistence_e2e.py` | Server crash mid-flight → DB has state → restart → "interrupted" → re-approve → completed |

## Configuration

All knobs come from env vars (with sensible defaults in `openmanus_server/config.py`):

| Var | Default | Effect |
|---|---|---|
| `OPENMANUS_WORKSPACE` | `./workspace` | where `design/`, `src/`, `tests/` live |
| `OPENMANUS_MINIMAX_BASE_URL` | `https://api.minimaxi.com/v1` | Architect LLM endpoint |
| `OPENMANUS_DEEPSEEK_BASE_URL` | `https://api.deepseek.com/v1` | Coder LLM endpoint |
| `OPENMANUS_ARCHITECT_MODEL` | `MiniMax-Text-01` | Architect model name |
| `OPENMANUS_CODER_MODEL` | `deepseek-chat` | Coder model name |
| `OPENMANUS_DB_PATH` | `./data/board.db` | SQLite database location |
| `MINIMAX_CN_API_KEY` | (required) | API key for Architect |
| `DEEPSEEK_API_KEY` | (required) | API key for Coder |

## Architecture decisions

- **Thin wrapper, not full OpenManus clone.** We don't pull in `browser-use`,
  Docker, or any other heavy deps. The pipeline is hard-coded A→B→A.
- **SQLite for persistence.** Single-file DB, no external service. WAL mode for
  concurrent reads.
- **In-process EventBus.** Pub-sub lives in `events.py`. If we ever need
  cross-process events, swap the implementation there — workflow.py doesn't care.
- **Server binds to 127.0.0.1 only.** No external exposure. For remote access,
  tunnel through your existing Caddy/cloudflared/etc.
- **Mock-first testing.** `openmanus_tests/mock_llm_server.py` is a tiny
  OpenAI-compatible stub used by all 3 e2e tests — no real API keys needed
  in CI.

## Commit history

```
1a4de1b P4: SQLite persistence + restart-recovery
bdf0c6d P3: UX polish — keyboard shortcuts, ACTION REQUIRED badge, no emoji tofu
1fcd34e P2: Dashboard frontend — fully dynamic UI
6cbcc2f P1: Board API + SSE event stream + in-memory store
009f812 Add 3 dashboard mockup iterations (v1 simple / v2 HSL tokens / v3 9/10 detail-rich)
dee2b70 Fix openmanus_status visibility + add mock-LLM e2e pipeline test
8ded01b Add MCP server exposing openmanus tools to Hermes
e0e50ef Refactor: generator-based agent loop + clean approval bridge
```

## License

This is a personal project; use as you see fit.