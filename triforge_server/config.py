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


def workspace_from_path(path: str) -> Path:
    """Use the given absolute path as the workspace root directly.

    Unlike workspace_for_run, this does NOT create a run_id subfolder —
    files are written straight to <path>/<rel_file>. This is used when
    the user specifies a project path in the New Task modal.
    """
    ws = Path(path).resolve()
    for sub in ("design", "src", "tests"):
        (ws / sub).mkdir(parents=True, exist_ok=True)
    return ws

# Two LLM providers. Each must have an OpenAI-compatible /v1/chat/completions endpoint.
LLM_PROVIDERS = {
    "minimax": {
        # Source: ~/.hermes/.env  MINIMAX_CN_BASE_URL=https://api.minimaxi.com/v1
        # Override via TRIFORGE_MINIMAX_BASE_URL for testing (e.g. mock LLM).
        "base_url": os.environ.get(
            "TRIFORGE_MINIMAX_BASE_URL",
            "https://api.minimaxi.com/v1",
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