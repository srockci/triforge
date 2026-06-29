# OpenManus Integration

A thin wrapper that exposes an A → B → A multi-agent pipeline (Architect-A designs, Coder-B implements, Architect-A reviews) as a local HTTP server, plus an MCP server so Hermes can call it from Telegram or CLI.

## Architecture

```
Hermes (TG / CLI)
  │  openmanus_start(requirement)
  ▼
MCP server (stdio) → FastAPI server (:8000)
                            │
                            ▼
                    Pipeline engine
                            │
                  ┌─────────┼─────────┐
                  ▼         ▼         ▼
              Architect-A  Coder-B  Architect-A
              (design)  →  (impl) →  (review)
                  │         │         │
                  ▼         ▼         ▼
            design/      src/       design/
            architecture.md *.py     review_report.md
```

Each phase has an approval gate: the pipeline pauses, exposes a preview to the API caller, and resumes when `/approve` is called.

## Layout

```
openmanus-integration/
├── openmanus_server/
│   ├── config.py         # LLM providers + agent system prompts
│   ├── agent.py          # Agent class — generator-based LLM loop with tool calling
│   ├── workflow.py       # Pipeline engine — A→B→A with approval gates
│   ├── server.py         # FastAPI server (port 8000)
│   └── mcp_server.py     # MCP server (stdio) — exposed to Hermes
├── openmanus_tests/
│   ├── mock_llm_server.py    # OpenAI-compatible fake LLM for e2e tests
│   └── test_pipeline_e2e.py  # End-to-end pipeline test
├── workspace/
│   ├── design/   # architecture.md, review_report.md
│   ├── src/      # *.py implementations
│   └── tests/    # test_*.py
├── run_mcp_server.sh   # Wrapper to set cwd before invoking MCP server
├── start.sh            # Convenience script to start the FastAPI server
└── .venv/              # Python 3.11 venv
```

## Running

```bash
cd /root/openmanus-integration
source .venv/bin/activate
uvicorn openmanus_server.server:app --host 127.0.0.1 --port 8000
```

The server exposes:
- `GET  /health`
- `POST /workflow/start`         — `{requirement: "..."}`
- `GET  /workflow/{run_id}/status`
- `POST /workflow/{run_id}/approve` — `{decision: "approve|reject|modify", comment: ""}`

## Hermes integration

`openmanus_server.mcp_server` is registered in `~/.hermes/config.yaml` under `mcp_servers.openmanus` and exposes three tools:
- `openmanus_start(requirement)`
- `openmanus_approve(run_id, decision, comment)`
- `openmanus_status(run_id)`

⚠️ **Stdio MCP servers in Hermes don't honor `cwd:`** — the wrapper script `run_mcp_server.sh` does the `cd` before exec'ing Python.

## Environment variables

| Var | Default | Purpose |
|-----|---------|---------|
| `OPENMANUS_MINIMAX_BASE_URL` | `https://api.minimaxi.com/v1` | Override for Architect-A |
| `OPENMANUS_DEEPSEEK_BASE_URL` | `https://api.deepseek.com/v1` | Override for Coder-B |
| `OPENMANUS_ARCHITECT_MODEL` | `MiniMax-Text-01` | Model name for architect |
| `OPENMANUS_CODER_MODEL` | `deepseek-chat` | Model name for coder |
| `OPENMANUS_WORKSPACE` | `/root/openmanus-integration/workspace` | Where files land |
| `MINIMAX_CN_API_KEY` | — | Architect-A's API key |
| `DEEPSEEK_API_KEY` | — | Coder-B's API key |

## Testing

End-to-end test using a mock LLM:

```bash
cd /root/openmanus-integration
source .venv/bin/activate
python -m openmanus_tests.test_pipeline_e2e
```

Expected output:
```
✓ mock LLM up
✓ OpenManus FastAPI server up
✓ workflow started: run_xxx
  → approval 1: write_file(design/architecture.md)
  → approval 2: write_file(src/hello.py)
  → approval 3: write_file(design/review_report.md)
✓ completed after 3 approvals
✓ all 4 expected files present
RESULT: ✅ PASS
```