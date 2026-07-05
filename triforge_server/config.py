"""Agent & LLM configuration.

Edit LLM_PROVIDERS below to match the keys already in ~/.hermes/.env.

The model names here MUST match what the provider's /v1/models returns.
For MiniMax CN (api.minimaxi.com/v1) valid model ids include:
    MiniMax-Text-01, MiniMax-M2.7, MiniMax-M2.5, etc.
For DeepSeek (api.deepseek.com/v1) valid model ids include:
    deepseek-chat, deepseek-reasoner
"""
import os
from pathlib import Path

# Where workspace files land. Default: <project_root>/workspace
# Override via TRIFORGE_WORKSPACE env var.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_ROOT = Path(os.environ.get(
    "TRIFORGE_WORKSPACE",
    str(_PROJECT_ROOT / "workspace"),
)).resolve()


def workspace_for_run(run_id: str) -> Path:
    """Return a per-run workspace directory, creating subdirs if needed.

    Each run gets its own isolated workspace under WORKSPACE_ROOT/<run_id>/
    so that concurrent runs never overwrite each other's files.
    """
    run_ws = (WORKSPACE_ROOT / run_id).resolve()
    for sub in ("design", "src", "tests"):
        (run_ws / sub).mkdir(parents=True, exist_ok=True)
    return run_ws

# Two LLM providers. Each must have an OpenAI-compatible /v1/chat/completions endpoint.
LLM_PROVIDERS = {
    "minimax": {
        # Source: ~/.hermes/.env  MINIMAX_CN_BASE_URL=https://api.minimax.chat/v1
        # Override via TRIFORGE_MINIMAX_BASE_URL for testing (e.g. mock LLM).
        "base_url": os.environ.get(
            "TRIFORGE_MINIMAX_BASE_URL",
            "https://api.minimax.chat/v1",
        ),
        "api_key_env": "MINIMAX_CN_API_KEY",
        "model": os.environ.get("TRIFORGE_ARCHITECT_MODEL", "MiniMax-Text-01"),
        "rate_in": 0.0,
        "rate_out": 0.0,
        "token_plan": True,
    },
    "deepseek": {
        "base_url": os.environ.get(
            "TRIFORGE_DEEPSEEK_BASE_URL",
            os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
        ),
        "api_key_env": "DEEPSEEK_API_KEY",
        "model": os.environ.get("TRIFORGE_CODER_MODEL", "deepseek-chat"),
        "rate_in": 0.5,
        "rate_out": 1.5,
        "token_plan": False,
    },
}

# Token plan models - models that use token-plan pricing instead of per-token cost
TOKEN_PLAN_MODELS = {
    "minimax": ["MiniMax-Text-01", "MiniMax-M3", "MiniMax-M2.7", "MiniMax-M2.5", "abab6.5s-chat", "abab6.5s"],
    "other": []  # Add other token-plan providers here
}

# Agent system prompts. Three roles in the pipeline:
AGENT_PROMPTS = {
    "architect_design": """You are Architect-A, a senior system architect.

Your ONLY job in this phase: design the system architecture for the user's
requirement, output a single Markdown file `architecture.md`, and stop.

## Output requirements
- File path: write to the EXACT path given in the user message (workspace/design/architecture.md)
- Format: Markdown, must include these sections in order:
  1. # <System Name>
  2. ## 1. Overview — one paragraph
  3. ## 2. Tech Stack — bullet list with versions
  4. ## 3. Modules — for each module: name, responsibility, key interfaces
  5. ## 4. Data Flow — sequence of steps for the main use case
  6. ## 5. File Layout — tree of files to be created
  7. ## 6. Acceptance Criteria — testable bullets
- Length: aim for 200-500 lines. Comprehensive but not bloated.
- Do NOT write any implementation code. No .py files. Design only.

## Tools you may use
- write_file(path, content) — to save your design document
- finish() — call this AFTER the file is written, to signal completion

When done, call finish() with a one-line summary.""",

    "coder_implement": """You are Coder-B, a senior Python developer.

Your ONLY job: read `workspace/design/architecture.md` and implement the code
exactly as specified.

## Output requirements
- File paths: write each file to the EXACT path given in the user message
  (typically workspace/src/*.py and workspace/tests/test_*.py)
- Follow the architecture's file layout EXACTLY. Do not invent new modules.
- Every Python file MUST have:
  - Module-level docstring
  - Type annotations on all function signatures
  - Docstrings on all public functions/classes
  - PEP 8 formatting
- For every module, write a corresponding test file under workspace/tests/
- Sensitive values (passwords, secrets, hostnames) MUST come from env vars
  via os.getenv(...) — never hardcoded.

## Tools you may use
- read_file(path) — to read architecture.md and any existing files
- write_file(path, content) — to create source and test files
- finish() — call this AFTER all files are written and self-tested""",

    "architect_review": """You are Architect-A again, this time in REVIEW mode.

Your ONLY job: read everything in workspace/design/ and workspace/src/, and
write a review report to `workspace/design/review_report.md`.

## Review checklist (each MUST have a section)
1. Architecture consistency — does the code match the architecture doc?
2. Module completeness — are all architecture modules implemented?
3. Code quality — type hints, docstrings, error handling, PEP 8
4. Test coverage — are there tests? Do they cover the main paths?
5. Security — any hardcoded secrets, SQL injection, unsafe shell calls?
6. Verdict — PASS / CONDITIONAL PASS / FAIL, plus 1-3 concrete follow-ups

## Output
- File path: workspace/design/review_report.md
- Length: 100-300 lines
- Be specific: cite file paths and line ranges when raising issues

## Tools
- read_file(path), write_file(path, content), finish(summary)"""
}

# Version Control Configuration
VERSION_CONTROL = {
    "enabled": True,
    "platforms": {
        "github": {
            "name": "GitHub",
            "api_url": "https://api.github.com",
            "auth_token_env": "GITHUB_TOKEN",
            "git_url": "https://github.com"
        },
        "gitee": {
            "name": "Gitee", 
            "api_url": "https://gitee.com/api/v5",
            "auth_token_env": "GITEE_TOKEN",
            "git_url": "https://gitee.com"
        },
        "gitlab": {
            "name": "GitLab",
            "api_url": "https://gitlab.com/api/v4",
            "auth_token_env": "GITLAB_TOKEN", 
            "git_url": "https://gitlab.com"
        },
        "custom_git": {
            "name": "Custom Git",
            "api_url": "",
            "auth_token_env": "",
            "git_url": ""
        }
    },
    "default_branch": "main",
    "commit_message": "TriForge auto push",
    "auto_push": False
}