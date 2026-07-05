"""Persistent settings manager for TriForge.

Settings are stored as JSON at <project_root>/data/settings.json.
The schema covers:
  - providers: LLM provider configurations (base URL, API key env)
  - roles: agent role → provider + model + system prompt mapping
  - pipeline_params: max_steps, temperature, max_tokens per phase

Settings are loaded at server start and can be hot-reloaded via the
board API (GET/POST /board/settings). Changes take effect on the
NEXT run — in-flight pipelines continue with their original config.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, List

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SETTINGS_PATH = _PROJECT_ROOT / "data" / "settings.json"


# ---------------------------------------------------------------------------
# Default settings — used on first launch or when settings.json is missing
# ---------------------------------------------------------------------------
DEFAULT_SETTINGS: Dict[str, Any] = {
    "language": "zh-CN",  # "en" | "zh-CN"
    "providers": {
        "minimax": {
            "name": "MiniMax",
            "base_url": "https://api.minimax.chat/v1",
            "api_key": "",
            "api_key_env": "MINIMAX_CN_API_KEY",
        },
        "deepseek": {
            "name": "DeepSeek",
            "base_url": "https://api.deepseek.com/v1",
            "api_key": "",
            "api_key_env": "DEEPSEEK_API_KEY",
        },
    },
    "roles": {
        "architect_design": {
            "name": "Architect (Design)",
            "provider": "minimax",
            "model": "MiniMax-Text-01",
            "prompt": """You are Architect-A, a senior system architect.

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
        },
        "coder_implement": {
            "name": "Coder (Implement)",
            "provider": "deepseek",
            "model": "deepseek-chat",
            "prompt": """You are Coder-B, a senior Python developer.

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
        },
        "architect_review": {
            "name": "Architect (Review)",
            "provider": "minimax",
            "model": "MiniMax-Text-01",
            "prompt": """You are Architect-A again, this time in REVIEW mode.

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
- read_file(path), write_file(path, content), finish(summary)""",
        },
    },
    "pipeline_params": {
        "design": {"max_steps": 12, "temperature": 0.2, "max_tokens": 4096},
        "implement": {"max_steps": 25, "temperature": 0.2, "max_tokens": 4096},
        "review": {"max_steps": 12, "temperature": 0.2, "max_tokens": 4096},
        
        # Token plan settings
        "token_plan": {
            "enabled": False,
            "window_hours": [0, 5, 10, 15, 20],  # 0:00, 5:00, 10:00, 15:00, 20:00
            "models": {}  # model_name: is_token_plan
        },
    },
    "approval": {
        # Paths where writes are auto-approved (no confirmation needed).
        # Relative to workspace root. e.g. ["src/", "tests/", "design/"]
        "working_paths": [],
        # Once a specific file path is approved in a run, remember it
        # and skip future approvals for the same path within that run.
        "remember_approved": True,
    },
    "notification_channels": [
        # Each entry is one channel:
        #   {
        #     "type": "feishu" | "dingtalk" | "wechatwork" | "telegram",
        #     "enabled": true | false,
        #     "mode": "simple" | "complex",   # simple=milestones only,
        #                                    # complex=every event
        #     "webhook_url": "...",           # feishu / dingtalk / wechatwork
        #     "secret": "...",                # dingtalk optional HMAC
        #     "bot_token": "...",             # telegram
        #     "chat_id": "...",               # telegram
        #     "at_all_on_error": false,       # prefix @all on errors
        #   }
        # Channels with enabled=false are kept in settings but ignored
        # at dispatch time, so users can toggle without re-entering URLs.
    ],
    "version_control": {
        "enabled": True,
        "platforms": {
            "github": {
                "name": "GitHub",
                "api_url": "https://api.github.com",
                "auth_token": "",
                "auth_token_env": "GITHUB_TOKEN",
                "username": "",
                "email": ""
            },
            "gitee": {
                "name": "Gitee",
                "api_url": "https://gitee.com/api/v5", 
                "auth_token": "",
                "auth_token_env": "GITEE_TOKEN",
                "username": "",
                "email": ""
            },
            "gitlab": {
                "name": "GitLab",
                "api_url": "https://gitlab.com/api/v4",
                "auth_token": "",
                "auth_token_env": "GITLAB_TOKEN",
                "username": "",
                "email": ""
            },
            "custom_git": {
                "name": "Custom Git",
                "api_url": "",
                "auth_token": "",
                "auth_token_env": "",
                "git_url": "",
                "username": "",
                "email": ""
            }
        },
        "repositories": [],
        "default_branch": "main",
        "commit_message": "TriForge auto push",
        "auto_push": False
    },
}


# ---------------------------------------------------------------------------
# Settings manager
# ---------------------------------------------------------------------------
class SettingsManager:
    """Load, validate, and persist settings as JSON."""

    def __init__(self, path: Path = _SETTINGS_PATH):
        self.path = path
        self._data: Dict[str, Any] = {}
        self.load()

    def load(self) -> Dict[str, Any]:
        """Load settings from disk. Falls back to defaults if file missing."""
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._data = {}
        else:
            self._data = {}
        # Merge with defaults (fill in any missing keys)
        self._data = _deep_merge(DEFAULT_SETTINGS, self._data)
        return self._data

    def save(self, data: Optional[Dict[str, Any]] = None) -> None:
        """Persist settings to disk."""
        if data is not None:
            self._data = _deep_merge(DEFAULT_SETTINGS, data)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    def get(self) -> Dict[str, Any]:
        """Return current settings (cached)."""
        return self._data

    def get_provider(self, key: str) -> Dict[str, Any]:
        """Return a single provider's config with env var overrides applied.

        Environment variables like TRIFORGE_MINIMAX_BASE_URL override
        the base_url from settings. This allows tests to redirect to mock
        LLM servers without modifying the settings file.
        """
        cfg = dict(self._data.get("providers", {}).get(key, {}))
        # Resolve api_key from env var if not set
        if not cfg.get("api_key"):
            api_key_env = cfg.get("api_key_env", "")
            if api_key_env:
                cfg["api_key"] = os.environ.get(api_key_env, "")
        # Check for env var override: TRIFORGE_{KEY}_BASE_URL
        env_key = f"TRIFORGE_{key.upper()}_BASE_URL"
        override_url = os.environ.get(env_key)
        if override_url:
            cfg["base_url"] = override_url
        return cfg

    def get_role(self, role: str) -> Dict[str, Any]:
        """Return a single role's config."""
        return self._data.get("roles", {}).get(role, {})

    def get_pipeline_params(self, phase: str) -> Dict[str, Any]:
        """Return pipeline params for a phase."""
        defaults = {"max_steps": 12, "temperature": 0.2, "max_tokens": 4096}
        return self._data.get("pipeline_params", {}).get(phase, defaults)

    def get_version_control_config(self) -> Dict[str, Any]:
        """Return version control configuration."""
        return self._data.get("version_control", {})

    def get_platform_config(self, platform: str) -> Dict[str, Any]:
        """Return a platform's configuration."""
        return self._data.get("version_control", {}).get("platforms", {}).get(platform, {})

    def update_platform_config(self, platform: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Update a platform's configuration."""
        vc_config = self._data.get("version_control", {})
        platforms = vc_config.get("platforms", {})
        platforms[platform] = config
        vc_config["platforms"] = platforms
        self._data["version_control"] = vc_config
        self.save()
        return self._data

    def add_repository(self, repo: Dict[str, Any]) -> Dict[str, Any]:
        """Add a repository to version control."""
        vc_config = self._data.get("version_control", {})
        repositories = vc_config.get("repositories", [])
        repositories.append(repo)
        vc_config["repositories"] = repositories
        self._data["version_control"] = vc_config
        self.save()
        return self._data

    def get_repositories(self) -> List[Dict[str, Any]]:
        """Get all repositories."""
        return self._data.get("version_control", {}).get("repositories", [])

    def remove_repository(self, repo_name: str) -> Dict[str, Any]:
        """Remove a repository."""
        vc_config = self._data.get("version_control", {})
        repositories = vc_config.get("repositories", [])
        repositories = [r for r in repositories if r.get("name") != repo_name]
        vc_config["repositories"] = repositories
        self._data["version_control"] = vc_config
        self.save()
        return self._data

    def update(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a partial update and save."""
        self._data = _deep_merge(self._data, patch)
        self.save()
        return self._data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _deep_merge(base: Dict, override: Dict) -> Dict:
    """Recursively merge override into base. Override wins on conflicts."""
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


# Module-level singleton
_manager: Optional[SettingsManager] = None


def get_settings() -> SettingsManager:
    global _manager
    if _manager is None:
        _manager = SettingsManager()
    return _manager


def reload_settings() -> SettingsManager:
    """Force-reload from disk."""
    global _manager
    _manager = SettingsManager()
    return _manager
