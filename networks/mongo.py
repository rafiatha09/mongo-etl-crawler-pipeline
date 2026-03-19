from __future__ import annotations

import json
from typing import Any

from domain.categories import DataCategory
from domain.exceptions import ImproperlyConfigured
from domain.models import ETLRunSummary, SourceDocument, UserRecord
from preprocessing.utils import split_user_full_name
from settings import settings

try:
    from pymongo import MongoClient
except ImportError as exc:  # pragma: no cover
    MongoClient = None
    _PYMONGO_IMPORT_ERROR = exc
else:
    _PYMONGO_IMPORT_ERROR = None


class MongoWarehouse:
    def __init__(self, mongodb_uri: str | None = None, database_name: str | None = None) -> None:
        if MongoClient is None:
            raise ImproperlyConfigured(
                "pymongo is required to run this ETL project. Install dependencies from requirements.txt."
            ) from _PYMONGO_IMPORT_ERROR

        self.client = MongoClient(mongodb_uri or settings.MONGODB_URI)
        self.db = self.client[database_name or settings.MONGODB_DATABASE]

    def ensure_indexes(self) -> None:
        self.db[DataCategory.USERS.value].create_index("full_name", unique=True)
        self.db[DataCategory.USERS.value].create_index("id", unique=True)

        for collection in (
            DataCategory.BLOGS,
            DataCategory.NEWS,
            DataCategory.GITHUB,
            DataCategory.RESEARCH_PAPERS,
            DataCategory.JOB_POSTINGS,
        ):
            self.db[collection.value].create_index("link", unique=True)
            self.db[collection.value].create_index("topic_query")
            self.db[collection.value].create_index("source_domain")
            self.db[collection.value].create_index("is_ai_related")

        self.db[DataCategory.ETL_RUNS.value].create_index("run_id", unique=True)
        self.db[DataCategory.ETL_RUNS.value].create_index("topic_query")

    def source_collections(self) -> tuple[DataCategory, ...]:
        return (
            DataCategory.BLOGS,
            DataCategory.NEWS,
            DataCategory.GITHUB,
            DataCategory.RESEARCH_PAPERS,
            DataCategory.JOB_POSTINGS,
        )

    def get_or_create_user(self, user_full_name: str) -> UserRecord:
        collection = self.db[DataCategory.USERS.value]
        existing = collection.find_one({"full_name": user_full_name})
        if existing:
            return UserRecord(**_strip_mongo_id(existing))

        first_name, last_name = split_user_full_name(user_full_name)
        user = UserRecord(full_name=user_full_name, first_name=first_name, last_name=last_name)
        collection.insert_one(user.to_mongo_document())
        return user

    def source_exists(self, collection_name: DataCategory, link: str) -> bool:
        collection = self.db[collection_name.value]
        return collection.find_one({"link": link}, {"_id": 1}) is not None

    def find_source_collection(self, link: str) -> DataCategory | None:
        for collection_name in self.source_collections():
            if self.source_exists(collection_name, link):
                return collection_name
        return None

    def insert_document(self, document: SourceDocument) -> str:
        collection = self.db[document.collection_name.value]
        payload = document.to_mongo_document()
        collection.insert_one(payload)
        return "inserted"

    def delete_source(self, collection_name: DataCategory, link: str) -> None:
        self.db[collection_name.value].delete_one({"link": link})

    def save_run_summary(self, summary: ETLRunSummary) -> None:
        self.db[DataCategory.ETL_RUNS.value].insert_one(summary.to_mongo_document())

    def preview_topic_analytics(self, topic_query: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        pipeline = build_market_intelligence_pipeline(topic_query=topic_query)
        preview: list[dict[str, Any]] = []
        for collection_name in (
            DataCategory.BLOGS.value,
            DataCategory.NEWS.value,
            DataCategory.GITHUB.value,
            DataCategory.RESEARCH_PAPERS.value,
            DataCategory.JOB_POSTINGS.value,
        ):
            collection = self.db[collection_name]
            preview.extend(collection.aggregate(pipeline))

        preview = sorted(preview, key=lambda item: item.get("document_count", 0), reverse=True)
        return pipeline, preview[: settings.MAX_ANALYTICS_RESULTS]


def build_market_intelligence_pipeline(topic_query: str | None = None) -> list[dict[str, Any]]:
    match_stage: dict[str, Any] = {"is_ai_related": True}
    if topic_query:
        match_stage["topic_query"] = topic_query

    return [
        {"$match": match_stage},
        {
            "$group": {
                "_id": {
                    "source_domain": "$source_domain",
                    "content_kind": "$content_kind",
                },
                "document_count": {"$sum": 1},
                "avg_ai_relevance_score": {"$avg": "$ai_relevance_score"},
                "avg_hiring_signal_score": {"$avg": "$hiring_signal_score"},
                "top_topics": {"$push": "$ai_topics"},
                "top_tags": {"$push": "$tags"},
            }
        },
        {"$sort": {"document_count": -1, "avg_ai_relevance_score": -1}},
        {"$limit": settings.MAX_ANALYTICS_RESULTS},
    ]


def _strip_mongo_id(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("_id", None)
    return result


def pretty_json(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)
