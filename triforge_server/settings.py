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
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, List

log = logging.getLogger("triforge.settings")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SETTINGS_PATH = _PROJECT_ROOT / "data" / "settings.json"


# ---------------------------------------------------------------------------
# Default settings — used on first launch or when settings.json is missing
# ---------------------------------------------------------------------------
DEFAULT_SETTINGS: Dict[str, Any] = {
    "language": "zh-CN",  # "en" | "zh-CN"
    "public_url": "http://localhost:8800",  # used for Telegram View diff button
    "providers": {
        "minimax": {
            "name": "MiniMax",
            "base_url": "https://api.minimaxi.com/v1",
            "api_key": "",
            "api_key_env": "MINIMAX_CN_API_KEY",
            "token_plan_mode": "charge",
            "available_models": [],
        },
        "deepseek": {
            "name": "DeepSeek",
            "base_url": "https://api.deepseek.com/v1",
            "api_key": "",
            "api_key_env": "DEEPSEEK_API_KEY",
            "token_plan_mode": "charge",
            "available_models": [],
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

        "module_detail": {
            "name": "Architect (Module Detail)",
            "provider": "minimax",
            "model": "MiniMax-Text-01",
            "prompt": """You are Architect-A designing a single module in detail.

Your ONLY job: write a detailed design document for the given module.

## Input
- User's original requirement
- Top-level architecture: design/architecture.md
- Current module's interface contract (must not change)
- Summary of already-completed modules

## Output — write to design/modules/<module_id>.md, include:
1. Function signatures (types, return, exceptions)
2. Class structure (fields, methods)
3. Data structures (dict/dataclass fields)
4. Error handling strategy
5. Edge cases (empty input, concurrency, timeout)
6. Interface contract with dependent modules

## Constraints
- Do NOT write any .py code
- Do NOT modify modules.json
- Do NOT modify design/architecture.md
- Read the top-level architecture first

When done, call finish(summary="detailed module <module_id>").""",
        },
        "module_code": {
            "name": "Coder (Module Code)",
            "provider": "deepseek",
            "model": "deepseek-chat",
            "prompt": """You are Coder-B implementing a single module.

Your ONLY job: implement ONE module based on its detailed design.

## Input
- User's original requirement
- Detailed design: design/modules/<module_id>.md
- Current module's interface contract (must not change)
- Summary of already-completed modules (import from real code, not stubs)

## What to produce
- Implementation files under src/<module_path>/*.py
- Test file tests/test_<module_id>.py

## STRICT constraints
- Write ONLY files belonging to this module — do NOT touch other modules
- Do NOT modify anything under design/
- Do NOT use stubs for dependencies from already-completed modules
- All file paths are RELATIVE to workspace root

## Correct paths
  - write_file path='src/<module>/__init__.py'
  - write_file path='src/<module>/core.py'
  - write_file path='tests/test_<module>.py'

Begin by reading design/modules/<module_id>.md. Call finish() when done.""",
        },
        # Only used when pipeline_params.module.reuse_designer_for_test=false
        # (default: true → uses architect_review instead)
        "module_test": {
            "name": "Test Agent (Module Diagnosis)",
            "provider": "minimax",
            "model": "MiniMax-Text-01",
            "prompt": """You are Test Agent. **Diagnose only — never modify code.**

## Input
Current module's implementation files and test file.

## Task
1. Read tests/test_<module_id>.py to see what the coder wrote
2. Read the module's implementation files under src/
3. Diagnose: would the tests pass against the implementation?
4. Output PASS or FAIL with specific errors

## IMPORTANT — read-only mode
- You MUST NOT call write_file under any circumstances
- Use read_file to examine code and tests
- Call finish(summary="PASS") or finish(summary="FAIL: <reason>")

## Checklist
- Do the tests cover the core logic?
- Are the imports correct (matching actual file paths)?
- Do function signatures in tests match the implementation?
- Are there obvious bugs in the implementation that tests would catch?

Be specific — cite file paths and line numbers when raising issues.""",
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
        "review": {"max_steps": 12, "temperature": 0.2, "max_tokens": 4096},
        # Module-level pipeline (modular design mode)
        "module": {
            "detail_max_steps": 8,
            "code_max_steps": 20,
            "test_max_steps": 6,
            "max_retry_per_module": 3,
            "estimated_files_max": 8,
            "estimated_steps_max": 22,
            "reuse_designer_for_test": True,
            "max_steps": 20,
            "temperature": 0.2,
            "max_tokens": 4096,
        },
        
        # Token plan settings
        "token_plan": {
            "enabled": False,
            "window_hours": [0, 5, 10, 15, 20],  # 0:00, 5:00, 10:00, 15:00, 20:00
            "alert_threshold_pct": 0.8,
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
        #     "webhook_url": "...",           # feishu / dingtalk / wechatwork / telegram
        #     "secret": "...",                # dingtalk optional HMAC
        #     "bot_token": "...",             # telegram
        #     "chat_id": "...",               # telegram
        #     "at_all_on_error": false,       # prefix @all on errors
        #     "webhook_secret": "",           # telegram: X-Telegram-Bot-Api-Secret-Token
        #     "polling_mode": true,           # telegram: false=webhook, true=polling
        #     "allowed_user_ids": [],          # telegram: list of ints, empty=allow all
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

    def get_public_url(self) -> str:
        return self._data.get("public_url", "http://localhost:8800")

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

    # -----------------------------------------------------------------------
    # Provider model & token-plan helpers (P1: provider_models_refactor)
    # -----------------------------------------------------------------------
    def get_provider_available_models(self, key: str) -> list:
        """Return the list of available models for a provider."""
        return self._data.get("providers", {}).get(key, {}).get("available_models", [])

    def set_provider_available_models(self, key: str, models: list) -> None:
        """Persist an updated available_models list for a provider.

        Auto-creates a provider stub if the key doesn't exist (e.g. user
        added a new provider but hasn't saved settings yet).
        """
        providers = self._data.setdefault("providers", {})
        if key not in providers:
            providers[key] = {
                "name": key.capitalize(),
                "base_url": "",
                "api_key": "",
                "api_key_env": key.upper() + "_API_KEY",
                "token_plan_mode": "charge",
                "available_models": [],
            }
            log.info("auto-created provider stub for %r", key)
        providers[key]["available_models"] = list(models)
        log.info("set_provider_available_models(%r) wrote %d models: %s",
                 key, len(models), models[:5])
        self.save()

    def get_provider_token_plan_mode(self, key: str) -> str:
        """Return the token_plan_mode for a provider: 'charge' | 'token_plan' | 'free'."""
        return self._data.get("providers", {}).get(key, {}).get("token_plan_mode", "charge")

    def set_provider_token_plan_mode(self, key: str, mode: str) -> None:
        """Set token_plan_mode for a provider."""
        if mode not in ("charge", "token_plan", "free"):
            raise ValueError(f"invalid token_plan_mode: {mode!r}, must be charge/token_plan/free")
        if key not in self._data.get("providers", {}):
            raise KeyError(f"unknown provider: {key}")
        self._data["providers"][key]["token_plan_mode"] = mode
        self.save()

    def get_model_token_plan_mode(self, provider_key: str, model_name: str) -> str:
        """Determine the effective token_plan_mode for a specific model.

        Returns the provider-level mode. Individual models inherit their
        provider's setting — there is no per-model override.
        """
        return self.get_provider_token_plan_mode(provider_key)

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

    def __getattr__(self, name: str) -> Any:
        """Allow attribute-style access to settings keys.

        e.g. settings.pipeline_params → self._data["pipeline_params"]
        """
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"SettingsManager has no key {name!r}")

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
