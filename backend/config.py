"""Typed application configuration loaded from the environment.

All settings are read via `pydantic-settings`. No module-level side effects —
callers must instantiate :func:`get_settings` (cached) to obtain a `Settings`
object. This keeps configuration testable and free of global state.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings backed by environment variables / ``.env``.

    Only keys that are already used in Phase 0 carry real defaults; keys for
    later phases are declared here with placeholder defaults so that the
    application still boots when they are absent.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- App ---
    app_env: str = Field(default="development")
    log_level: str = Field(default="INFO")

    # --- Database ---
    database_url: str = Field(
        default="postgresql+asyncpg://keystone:keystone@localhost:5432/keystone"
    )
    database_url_sync: str = Field(
        default="postgresql://keystone:keystone@localhost:5432/keystone"
    )

    # --- LLM ---
    # Which backend serves every LLM call (see backend/services/llm/). With no
    # key for the selected provider, features use their deterministic
    # fallbacks; Ask and reply drafting return 503.
    llm_provider: Literal["gemini", "pioneer"] = Field(default="gemini")
    gemini_api_key: str = Field(default="")
    # Explicit model versions keep eval runs comparable over time.
    gemini_flash_model: str = Field(default="gemini-2.5-flash")
    gemini_pro_model: str = Field(default="gemini-2.5-pro")
    pioneer_api_key: str = Field(default="")
    pioneer_model: str = Field(default="claude-sonnet-4-6")

    # --- IMAP / Slack / Mock ERP (used by later phases) ---
    imap_host: str = Field(default="imap.gmail.com")
    imap_port: int = Field(default=993)
    imap_user: str = Field(default="")
    imap_password: str = Field(default="")
    imap_mailbox: str = Field(default="INBOX")
    slack_signing_secret: str = Field(default="")
    # Base URL of an ERP payments feed (see tools/mock_erp). Empty disables
    # the ERP poller entirely, so a default install makes no outbound calls.
    mock_erp_url: str = Field(default="")

    # --- Signals ---
    # After a signal is approved or rejected, the evaluator will not raise
    # the same finding (same natural key) again for this many days.
    signal_resolution_cooldown_days: int = Field(default=7, ge=0)

    # --- Phase 10 demo ---
    # Hero property the no-arg ``POST /admin/demo/replay`` runs against.
    # Default: WE 29 (Step 6 hero, see docs/DECISIONS.md). Override via env when
    # a different property has richer coverage post-hackathon.
    keystone_demo_hero_property: str = Field(
        default="509393da-6806-49ef-9e59-3da0213008cd"
    )
    # Replay engine speed multiplier the demo uses by default.
    keystone_demo_speed_multiplier: int = Field(default=10)
    # ``hand_crafted`` falls back to the Berliner 4B beats for safety;
    # ``buena`` is the primary file-builds-itself replay.
    keystone_demo_mode: str = Field(default="buena")

    # --- Phase 10 Step 10.4 — document linking ---
    # Filesystem root that ``GET /files/<path>`` is allowed to serve. Any
    # path that, after ``realpath`` resolution, escapes this root is
    # refused with 403. Default points at the gitignored Buena dump so
    # the dev environment "just works"; production deployments must
    # set an absolute path explicitly.
    keystone_files_root: str = Field(default="Extracted")

    # --- Browser access (CORS) ---
    # Comma-separated allow-list of browser origins; the only source of CORS
    # origins. The default covers local frontends: the Vite dev server
    # (8080 as configured, 5173 Vite's default), Vite preview (4173) and
    # 3000. Deployments set their frontend origin explicitly, e.g.
    # ``KEYSTONE_CORS_ORIGINS=https://app.example.com``.
    keystone_cors_origins: str = Field(
        default=(
            "http://localhost:5173,http://localhost:4173,http://localhost:3000,"
            "http://localhost:8080,http://localhost:8081,http://localhost:8082"
        )
    )

    def cors_origin_list(self) -> list[str]:
        """``keystone_cors_origins`` split on commas, blanks dropped."""
        return [o.strip() for o in self.keystone_cors_origins.split(",") if o.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide cached :class:`Settings` instance."""
    return Settings()
