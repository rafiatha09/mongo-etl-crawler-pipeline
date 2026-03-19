from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    MONGODB_URI: str = "mongodb://localhost:27017"
    MONGODB_DATABASE: str = "market_intelligence"

    REQUEST_TIMEOUT_SECONDS: int = 20
    DISCOVERY_TIMEOUT_SECONDS: int = 12
    DISCOVERY_MAX_LINKS: int = 25
    DISCOVERY_MIN_PER_CATEGORY: int = 5
    USER_AGENT: str = "market-intelligence-etl/1.0 (+local)"
    LINKEDIN_JOB_LOCATION: str = "Worldwide"
    LINKEDIN_JOB_PAGE_SIZE: int = 25
    LINKEDIN_JOB_MAX_PAGES: int = 2
    GITHUB_MAX_FILES: int = 120
    GITHUB_MAX_FILE_CHARS: int = 4000
    GITHUB_MAX_TOTAL_CHARS: int = 300000
    GITHUB_SKIP_FILE_BYTES: int = 262144

    MAX_CONTENT_CHARS: int = 25_000
    MIN_EXTRACTED_CONTENT_CHARS: int = 120
    MAX_SUMMARY_CHARS: int = 600
    MAX_TAGS: int = 12
    MAX_ANALYTICS_RESULTS: int = 10

    GITHUB_API_TOKEN: str | None = None
    EXTRA_HEADERS_JSON: str | None = None

    DEFAULT_USER_FULL_NAME: str = "AI Engineer"
    DEFAULT_TOPIC_QUERY: str = "agentic ai"
    DEFAULT_LINKS: list[str] = Field(default_factory=list)


settings = Settings()
