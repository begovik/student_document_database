import os
from pathlib import Path
from typing import Annotated, Any

import yaml
from pydantic import AliasChoices, BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ContactConfig(BaseModel):
    email: str


class PathsConfig(BaseModel):
    db_path: str = "data/harvester.db"
    tmp_dir: str = "data/tmp"
    backup_dir: str = "backups"


class WorkersConfig(BaseModel):
    verify: int = Field(default=6, ge=1, le=32)
    discovery: int = Field(default=3, ge=1, le=16)
    scanner: int = Field(default=1, ge=1, le=8)
    classify: int = Field(default=1, ge=1, le=4)


class HttpConfig(BaseModel):
    global_concurrency: int = Field(default=32, ge=1, le=200)
    per_host_delay_ms: int = Field(default=2000, ge=100, le=30000)
    per_host_burst: int = Field(default=2, ge=1, le=10)
    max_pdf_bytes: int = Field(default=209715200, ge=10240)
    min_pdf_bytes: int = Field(default=10240, ge=1024)
    max_redirects: int = Field(default=5, ge=0, le=20)
    connect_timeout_s: float = Field(default=10.0, gt=0)
    read_timeout_s: float = Field(default=30.0, gt=0)
    total_timeout_s: float = Field(default=300.0, gt=0)
    bandwidth_mbps: float = Field(default=2.0, gt=0, le=100.0)
    user_agent: str = "Harvester/1.0"


class DDGSChannelConfig(BaseModel):
    enabled: bool = True
    backends: list[str] = Field(
        default=["duckduckgo", "bing", "brave", "mojeek", "startpage", "yahoo", "wikipedia"]
    )
    query_interval_s: tuple[float, float] = (3.0, 10.0)


class SimpleChannelConfig(BaseModel):
    enabled: bool = True
    rps: float = Field(default=1.0, gt=0, le=20)


class OAIChannelConfig(BaseModel):
    enabled: bool = True
    per_host_interval_s: float = Field(default=2.0, gt=0, le=30)


class ChannelsConfig(BaseModel):
    ddgs: DDGSChannelConfig = Field(default_factory=DDGSChannelConfig)
    openalex: SimpleChannelConfig = Field(default_factory=lambda: SimpleChannelConfig(rps=5))
    crossref: SimpleChannelConfig = Field(default_factory=lambda: SimpleChannelConfig(rps=5))
    unpaywall: SimpleChannelConfig = Field(default_factory=SimpleChannelConfig)
    semanticscholar: SimpleChannelConfig = Field(default_factory=SimpleChannelConfig)
    arxiv: SimpleChannelConfig = Field(default_factory=lambda: SimpleChannelConfig(rps=0.33))
    doaj: SimpleChannelConfig = Field(default_factory=SimpleChannelConfig)
    internet_archive: SimpleChannelConfig = Field(default_factory=SimpleChannelConfig)
    core: SimpleChannelConfig = Field(default_factory=lambda: SimpleChannelConfig(enabled=False))
    oai: OAIChannelConfig = Field(default_factory=OAIChannelConfig)


class FiltersConfig(BaseModel):
    blocked_tlds: list[str] = Field(default=[".ru", ".su", ".рф", ".xn--p1ai"])
    soviet_cutoff_year: int = Field(default=1991, ge=1900, le=2000)
    soviet_action: str = Field(default="reject", pattern=r"^(reject|flag)$")
    ru_lang_min_confidence: float = Field(default=0.8, ge=0.0, le=1.0)


class VerifyConfig(BaseModel):
    first_pages_for_text: int = Field(default=3, ge=1, le=20)
    title_match_min: int = Field(default=85, ge=0, le=100)
    title_match_review: int = Field(default=60, ge=0, le=100)
    max_pages: int = Field(default=2000, ge=1)
    max_attempts: int = Field(default=3, ge=1, le=10)


class ClassifyConfig(BaseModel):
    min_score: float = Field(default=0.25, ge=0.0, le=1.0)
    max_topics: int = Field(default=3, ge=1, le=10)


class ReverifyConfig(BaseModel):
    interval_days_new: int = Field(default=30, ge=1, le=365)
    interval_days_stable: int = Field(default=90, ge=1, le=365)
    broken_retry_days: int = Field(default=3, ge=1, le=30)


class ScannerConfig(BaseModel):
    max_pages_per_source: int = Field(default=300, ge=1, le=10000)
    max_depth: int = Field(default=2, ge=1, le=5)
    sitemap_max_urls: int = Field(default=50000, ge=100, le=1000000)


class RetentionConfig(BaseModel):
    fetch_attempts_days: int = Field(default=90, ge=1, le=3650)
    events_days: int = Field(default=180, ge=1, le=3650)
    backups_keep: int = Field(default=14, ge=1, le=365)


class LoggingConfig(BaseModel):
    level: str = Field(default="INFO", pattern=r"^(DEBUG|INFO|WARN|ERROR)$")
    file: str | None = "logs/harvester.log"


class DatabaseConfig(BaseModel):
    """Підключення до віддаленої PostgreSQL з локальним SQLite-failover."""

    mode: str = Field(default="auto", pattern=r"^(auto|remote|local)$")
    host: str = ""
    port: int = Field(default=5432, ge=1, le=65535)
    name: str = "harvester"
    user: str = "harvester"
    dsn: str | None = None
    pool_min_size: int = Field(default=1, ge=1, le=50)
    pool_max_size: int = Field(default=20, ge=1, le=200)
    connect_timeout_s: float = Field(default=5.0, gt=0)
    retries: int = Field(default=3, ge=1, le=10)
    retry_delay_s: float = Field(default=2.0, gt=0)
    restore_probe_interval_s: float = Field(default=30.0, gt=0)
    merge_on_restore: bool = True
    local_db_path: str = "data/harvester.db"

    @property
    def remote_configured(self) -> bool:
        return bool(self.host) or bool(self.dsn)


class LLMConfig(BaseModel):
    enabled: bool = True
    gemini_models: list[str] = Field(default=["gemini-3.1-flash-lite", "gemini-3.5-flash-lite"])
    gemma_models: list[str] = Field(default=["gemma-4-31b-it", "gemma-4-26b-it"])
    gemini_base_url: str = "https://generativelanguage.googleapis.com/v1beta"
    gemma_max_chars: int = Field(default=15000, ge=5000, le=100000)
    openrouter_model: str = "google/gemini-2.5-flash"
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    timeout_s: float = Field(default=60.0, gt=0)
    max_tokens: int = Field(default=2048, ge=256, le=8192)
    temperature: float = Field(default=0.1, ge=0.0, le=2.0)
    min_interval_s: float = Field(default=1.5, ge=0.0)
    daily_limit_wait_s: float = Field(default=120.0, gt=0)
    max_text_chars_for_llm: int = Field(default=80000, ge=10000, le=500000)
    max_pages_for_extraction: int = Field(default=100, ge=10, le=1000)


class NotifyConfig(BaseModel):
    """Конфігурація сповіщень на пошту."""
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = Field(default=587, ge=1, le=65535)
    smtp_starttls: bool = True
    smtp_user: str = ""
    smtp_password: str = ""
    from_email: str = ""
    to_email: str = ""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HARVESTER_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
        env_file=".env",
        env_file_encoding="utf-8",
    )

    contact: ContactConfig
    paths: PathsConfig = Field(default_factory=PathsConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    workers: WorkersConfig = Field(default_factory=WorkersConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    channels: ChannelsConfig = Field(default_factory=ChannelsConfig)
    filters: FiltersConfig = Field(default_factory=FiltersConfig)
    verify: VerifyConfig = Field(default_factory=VerifyConfig)
    classify: ClassifyConfig = Field(default_factory=ClassifyConfig)
    reverify: ReverifyConfig = Field(default_factory=ReverifyConfig)
    scanner: ScannerConfig = Field(default_factory=ScannerConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    notify: NotifyConfig = Field(default_factory=NotifyConfig)

    s2_api_key: Annotated[str | None, Field(default=None)] = None
    core_api_key: Annotated[str | None, Field(default=None)] = None
    gemini_api_key: Annotated[str | None, Field(default=None, validation_alias="GEMINI_API_KEY")] = None
    gemini_api_key_2: Annotated[str | None, Field(default=None, validation_alias="GEMINI_API_KEY_2")] = None
    gemini_api_key_3: Annotated[str | None, Field(default=None, validation_alias="GEMINI_API_KEY_3")] = None
    open_router_api_key: Annotated[str | None, Field(default=None, validation_alias="OPEN_ROUTER_API_KEY")] = None
    pg_user: Annotated[str | None, Field(default=None, validation_alias=AliasChoices("HARVESTER_PG_USER", "PG_USER"))] = None
    pg_password: Annotated[str | None, Field(default=None, validation_alias=AliasChoices("HARVESTER_PG_PASSWORD", "PG_PASS"))] = None
    user_email: Annotated[str | None, Field(default=None, validation_alias="USER_EMAIL")] = None
    smtp_password: Annotated[str | None, Field(default=None, validation_alias="HARVESTER_SMTP_PASSWORD")] = None

    @property
    def gemini_keys(self) -> list[str]:
        return [k for k in (self.gemini_api_key, self.gemini_api_key_2, self.gemini_api_key_3) if k]

    @field_validator("contact")
    @classmethod
    def validate_contact(cls, v: ContactConfig) -> ContactConfig:
        if not v.email or v.email == "you@example.org":
            pass
        return v

    @property
    def db_path(self) -> Path:
        return Path(self.paths.db_path)

    @property
    def tmp_dir(self) -> Path:
        return Path(self.paths.tmp_dir)

    @property
    def backup_dir(self) -> Path:
        return Path(self.paths.backup_dir)


def load_config(config_path: str | Path | None = None) -> Settings:
    if config_path is None:
        config_path = Path("config.yaml")
    else:
        config_path = Path(config_path)

    data: dict[str, Any] = {}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

    settings = Settings(**data)

    # Заповнити NotifyConfig з user_email та smtp_password
    if settings.user_email:
        settings.notify.smtp_user = settings.notify.smtp_user or settings.user_email
        settings.notify.from_email = settings.notify.from_email or settings.user_email
        settings.notify.to_email = settings.notify.to_email or settings.user_email
        # Автоматично увімкнути сповіщення якщо є email та SMTP пароль
        if settings.smtp_password and not settings.notify.smtp_host:
            settings.notify.smtp_host = "smtp.gmail.com"
            settings.notify.enabled = True
    if settings.smtp_password:
        settings.notify.smtp_password = settings.smtp_password

    return settings


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = load_config()
    return _settings


def reload_settings(config_path: str | Path | None = None) -> Settings:
    global _settings
    _settings = load_config(config_path)
    return _settings


# === Правила фільтрації документів ===

class FilterRules(BaseModel):
    """Правила фільтрації документів для каталогів."""
    description: str = ""
    min_page_count: int = Field(default=1, ge=1, le=100)
    require_references: bool = False
    require_structured_sections: bool = False
    require_title_page: bool = False
    llm_completeness_level: str = Field(default="basic", pattern=r"^(basic|strict)$")


_rules_cache: dict[str, FilterRules] | None = None
_rules_config: dict[str, Any] | None = None


def _load_rules_config() -> dict[str, Any]:
    """Завантажити конфігурацію правил з rules.yaml."""
    global _rules_config
    if _rules_config is None:
        rules_path = Path(__file__).parent / "config" / "rules.yaml"
        if rules_path.exists():
            with open(rules_path, "r", encoding="utf-8") as f:
                _rules_config = yaml.safe_load(f) or {}
        else:
            _rules_config = {"profiles": {}, "active_profile": "strict"}
    return _rules_config


def get_filter_rules(profile: str | None = None) -> FilterRules:
    """Отримати правила фільтрації для вказаного профілю."""
    global _rules_cache
    if _rules_cache is None:
        _rules_cache = {}
        config = _load_rules_config()
        profiles = config.get("profiles", {})
        for name, data in profiles.items():
            _rules_cache[name] = FilterRules(**data)

    if profile is None:
        config = _load_rules_config()
        profile = config.get("active_profile", "strict")

    if profile not in _rules_cache:
        available = list(_rules_cache.keys())
        raise ValueError(f"Невідомий профіль '{profile}'. Доступні: {available}")

    return _rules_cache[profile]


def reload_rules() -> None:
    """Перезавантажити правила з файлу."""
    global _rules_cache, _rules_config
    _rules_cache = None
    _rules_config = None
