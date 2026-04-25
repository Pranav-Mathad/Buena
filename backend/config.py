"""Typed application configuration loaded from the environment.

All settings are read via `pydantic-settings`. No module-level side effects —
callers must instantiate :func:`get_settings` (cached) to obtain a `Settings`
object. This keeps configuration testable and free of global state.
"""

from __future__ import annotations

from functools import lru_cache

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

    # --- Gemini ---
    gemini_api_key: str = Field(default="")
    # Model defaults updated 2026-04-25 — Phase 8.1. The previous
    # ``gemini-2.0-{flash,pro}`` names were retired for new API users.
    # Pinning explicit versions makes eval/runs reproducible across
    # weeks; flip to ``gemini-flash-latest`` if we explicitly want
    # tracking-the-edge behavior.
    gemini_flash_model: str = Field(default="gemini-2.5-flash")
    gemini_pro_model: str = Field(default="gemini-2.5-pro")
    gemini_embed_model: str = Field(default="text-embedding-004")

    # --- Partners ---
    tavily_api_key: str = Field(default="")
    entire_api_key: str = Field(default="")
    entire_base_url: str = Field(default="https://api.entire.example")
    pioneer_api_key: str = Field(default="")
    aikido_api_key: str = Field(default="")
    gradium_api_key: str = Field(default="")

    # --- IMAP / Slack / Mock ERP (used by later phases) ---
    imap_host: str = Field(default="imap.gmail.com")
    imap_port: int = Field(default=993)
    imap_user: str = Field(default="")
    imap_password: str = Field(default="")
    imap_mailbox: str = Field(default="INBOX")
    slack_signing_secret: str = Field(default="")
    mock_erp_url: str = Field(default="http://localhost:8001")

    # --- Phase 10 demo ---
    # Hero property the no-arg ``POST /admin/demo/replay`` runs against.
    # Default: WE 29 (Step 6 hero, see DECISIONS.md). Override via env when
    # a different property has richer coverage post-hackathon.
    keystone_demo_hero_property: str = Field(
        default="509393da-6806-49ef-9e59-3da0213008cd"
    )
    # Replay engine speed multiplier the demo uses by default.
    keystone_demo_speed_multiplier: int = Field(default=10)
    # ``hand_crafted`` falls back to the Berliner 4B beats for safety;
    # ``buena`` is the primary file-builds-itself replay.
    keystone_demo_mode: str = Field(default="buena")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide cached :class:`Settings` instance."""
    return Settings()
