from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from .categories import DataCategory


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_for_mongo(value: Any) -> Any:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_normalize_for_mongo(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize_for_mongo(item) for key, item in value.items()}
    return value


class UserRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    full_name: str
    first_name: str
    last_name: str
    created_at: datetime = Field(default_factory=utc_now)

    def to_mongo_document(self) -> dict[str, Any]:
        return _normalize_for_mongo(self.model_dump(mode="python"))


class SourceDocument(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    collection_name: DataCategory
    title: str
    content: str
    summary: str | None = None
    published_at: datetime | None = None
    source: str
    source_domain: str
    link: str
    platform: str
    topic_query: str
    tags: list[str] = Field(default_factory=list)
    content_kind: str = "general"
    is_ai_related: bool = False
    ai_relevance_score: float = 0.0
    ai_topics: list[str] = Field(default_factory=list)
    ai_keywords: list[str] = Field(default_factory=list)
    has_job_signal: bool = False
    job_roles: list[str] = Field(default_factory=list)
    hiring_signal_score: float = 0.0
    created_by_user_id: str
    created_by_user_name: str
    raw_metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    def to_mongo_document(self) -> dict[str, Any]:
        payload = self.model_dump(mode="python")
        payload["collection_name"] = self.collection_name.value
        return _normalize_for_mongo(payload)


class CrawlResult(BaseModel):
    status: str
    link: str
    collection_name: DataCategory | None = None
    document: SourceDocument | None = None
    reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ETLRunSummary(BaseModel):
    run_id: str = Field(default_factory=lambda: str(uuid4()))
    topic_query: str
    user_id: str
    user_full_name: str
    start_date: date
    end_date: date
    discovered_links: list[str]
    started_at: datetime
    finished_at: datetime
    saved_count: int
    moved_count: int
    duplicate_count: int
    skipped_count: int
    error_count: int
    per_collection: dict[str, int]
    per_domain: dict[str, dict[str, Any]]
    analytics_pipeline: list[dict[str, Any]]
    analytics_preview: list[dict[str, Any]] = Field(default_factory=list)

    def to_mongo_document(self) -> dict[str, Any]:
        return _normalize_for_mongo(self.model_dump(mode="python"))
