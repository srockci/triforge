"""MCP server wrapping the TriForge integration.

Exposes three tools over MCP (Model Context Protocol, stdio transport):

  - triforge_start(requirement: str)
  - triforge_approve(run_id: str, decision: str, comment: str = "")
  - triforge_status(run_id: str)

Hermes auto-discovers any MCP server registered in config.yaml under
mcp_servers and surfaces its tools to the LLM.

Run standalone for debugging:
    # Linux / macOS
    cd <project_root> && source .venv/bin/activate
    python -m triforge_server.mcp_server

    # Windows
    .venv/Scripts/python -X utf8 -m triforge_server.mcp_server
"""
from __future__ import annotations

import os
import sys
import time
import json
from typing import Any

import requests
from mcp.server.fastmcp import FastMCP

TRIFORGE_URL = os.environ.get("TRIFORGE_URL", "http://127.0.0.1:8000").rstrip("/")
POLL_INTERVAL = float(os.environ.get("TRIFORGE_POLL_INTERVAL", "3"))
APPROVAL_TIMEOUT = float(os.environ.get("TRIFORGE_APPROVAL_TIMEOUT", "600"))


mcp = FastMCP("triforge")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _post(path: str, body: dict, timeout: float = 30) -> dict:
    r = requests.post(f"{TRIFORGE_URL}{path}", json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _get(path: str, timeout: float = 10) -> dict:
    r = requests.get(f"{TRIFORGE_URL}{path}", timeout=timeout)
    r.raise_for_status()
    return r.json()


def _format_outputs(outputs: dict) -> str:
    lines = []
    if outputs.get("design_doc"):
        lines.append(f"Design: `{outputs['design_doc']}`")
    if outputs.get("code_files"):
        lines.append("Code files:")
        for f in outputs["code_files"]:
            lines.append(f"  - `{f}`")
    if outputs.get("review_report"):
        lines.append(f"Review: `{outputs['review_report']}`")
    return "\n".join(lines) if lines else "(no outputs)"


def _format_approval_request(snap: dict) -> str:
    return (
        f"⏸️  **Approval Required**\n"
        f"   Phase: `{snap.get('phase', '?')}`\n"
        f"   Tool: `{snap.get('pending_tool', '?')}`\n"
        f"   Args: `{json.dumps(snap.get('pending_args', {}), ensure_ascii=False)[:200]}`\n\n"
        f"**Preview:**\n```\n"
        f"{(snap.get('pending_preview', '') or '')[:1200]}\n"
        f"```\n\n"
        f"Reply with one of:\n"
        f"  `/triforge_approve {snap.get('run_id')} approve`\n"
        f"  `/triforge_approve {snap.get('run_id')} reject <reason>`\n"
        f"  `/triforge_approve {snap.get('run_id')} modify <feedback>`"
    )


def _poll_until_pause_or_done(run_id: str) -> str:
    deadline = time.time() + APPROVAL_TIMEOUT
    while time.time() < deadline:
        try:
            snap = _get(f"/workflow/{run_id}/status")
        except Exception as e:
            return f"❌ Status check failed: {type(e).__name__}: {e}"

        status = snap.get("status")
        if status == "awaiting_approval":
            return _format_approval_request(snap)
        if status == "completed":
            return (
                f"✅ **Workflow Complete**\n\n"
                f"{_format_outputs(snap.get('outputs', {}))}\n\n"
                f"_run_id: `{run_id}`_"
            )
        if status == "failed":
            return (
                f"❌ **Workflow Failed**\n"
                f"   Error: `{snap.get('error', 'unknown')}`\n"
                f"   Phase: `{snap.get('phase', '?')}`\n\n"
                f"_run_id: `{run_id}`_"
            )
        time.sleep(POLL_INTERVAL)

    return (
        f"⏱️  Timed out waiting for run `{run_id}` after {APPROVAL_TIMEOUT:.0f}s. "
        f"Check status with `triforge_status({run_id!r})`."
    )


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@mcp.tool()
def triforge_start(requirement: str) -> str:
    """Start an TriForge dev pipeline (Architect-A designs, Coder-B implements, Architect-A reviews).

    Blocks until the first approval gate or completion. The user should then
    reply with `/triforge_approve <run_id> <decision>`.

    IMPORTANT: This tool accepts input ONLY from a trusted personal agent.
    Do NOT expose this MCP server to untrusted MCP clients — the requirement
    string is passed directly to the LLM pipeline with basic sanitization only.

    Args:
        requirement: Full user requirement description (include tech stack, features)
    """
    if len(requirement) > 10000:
        return "❌ requirement too long (max 10 000 chars)"
    import re
    # Strip control characters (except newline/tab) to reduce injection risk
    requirement = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', requirement)
    try:
        resp = _post("/workflow/start", {"requirement": requirement})
    except Exception as e:
        return f"❌ Failed to start TriForge: {type(e).__name__}: {e}"
    return _poll_until_pause_or_done(resp["run_id"])


@mcp.tool()
def triforge_approve(run_id: str, decision: str, comment: str = "") -> str:
    """Resume a paused TriForge run.

    Args:
        run_id: The run ID returned by triforge_start
        decision: One of "approve", "reject", or "modify"
        comment: Optional reason / feedback (required for reject/modify)
    """
    if decision not in ("approve", "reject", "modify"):
        return f"❌ decision must be approve|reject|modify, got {decision!r}"
    try:
        _post(f"/workflow/{run_id}/approve", {"decision": decision, "comment": comment})
    except Exception as e:
        return f"❌ Failed to approve: {type(e).__name__}: {e}"
    return _poll_until_pause_or_done(run_id)


@mcp.tool()
def triforge_status(run_id: str) -> str:
    """Snapshot of a run's current state (non-blocking).

    Args:
        run_id: The run ID to query
    """
    try:
        snap = _get(f"/workflow/{run_id}/status")
    except Exception as e:
        return f"❌ Failed to get status: {type(e).__name__}: {e}"
    return json.dumps(snap, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()  # stdio transport by default