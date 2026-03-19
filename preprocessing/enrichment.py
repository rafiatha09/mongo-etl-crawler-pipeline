from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from domain.constants import (
    AI_TOPIC_KEYWORDS,
    BLOG_DOMAINS,
    CONTENT_KIND_KEYWORDS,
    JOB_MARKET_KEYWORDS,
    JOB_DOMAINS,
    JOB_ROLE_KEYWORDS,
    JOB_SIGNAL_TITLE_KEYWORDS,
    NEWS_DOMAINS,
    NEWS_SIGNAL_KEYWORDS,
    PREDEFINED_SOURCE_LINKS,
    RESEARCH_DOMAINS,
    SOCIAL_DOMAINS,
)
from domain.categories import DataCategory
from domain.models import SourceDocument
from preprocessing.utils import safe_truncate
from settings import settings


def content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(content_to_text(item) for item in content)
    if isinstance(content, dict):
        return " ".join(content_to_text(value) for value in content.values())
    return str(content)


def normalize_tags(*groups: list[str] | set[str] | tuple[str, ...]) -> list[str]:
    tags: list[str] = []
    for group in groups:
        for item in group:
            parsed = item.strip().lower()
            if parsed and parsed not in tags:
                tags.append(parsed)
            if len(tags) >= settings.MAX_TAGS:
                return tags
    return tags


def summarize_text(text: str) -> str | None:
    stripped = " ".join(text.split())
    if not stripped:
        return None
    if len(stripped) <= settings.MAX_SUMMARY_CHARS:
        return stripped
    return stripped[: settings.MAX_SUMMARY_CHARS].rstrip() + "..."


def build_topic_keywords(topic_query: str) -> list[str]:
    normalized_topic = topic_query.strip().lower()
    if not normalized_topic:
        return []

    tokens = [token for token in re.split(r"\W+", normalized_topic) if len(token) > 2]
    keywords = set(tokens)
    keywords.add(normalized_topic)

    if "ai" in normalized_topic or "artificial intelligence" in normalized_topic:
        keywords.update(
            {
                "ai",
                "artificial intelligence",
                "machine learning",
                "generative ai",
                "agentic ai",
                "llm",
                "foundation model",
            }
        )

    return sorted(keywords)


def infer_content_kind(link: str, text: str) -> str:
    domain = urlparse(link).netloc.lower()
    if "github.com" in domain:
        return "project"
    if domain in SOCIAL_DOMAINS:
        return "impact"

    scores = {kind: sum(1 for keyword in keywords if keyword in text) for kind, keywords in CONTENT_KIND_KEYWORDS.items()}
    best_kind, best_score = max(scores.items(), key=lambda item: item[1], default=("general", 0))
    if best_score == 0:
        return "general"
    return best_kind


def infer_source_category(
    link: str,
    title: str,
    content: str,
    content_kind: str,
    has_job_signal: bool,
) -> DataCategory:
    domain = urlparse(link).netloc.lower()
    path = urlparse(link).path.lower()
    title_text = title.lower()
    content_text = content.lower()

    if "github.com" in domain:
        return DataCategory.GITHUB
    if domain in RESEARCH_DOMAINS or any(token in path for token in ("/abs/", "/pdf/", "/paper", "/papers", "/research")):
        return DataCategory.RESEARCH_PAPERS

    looks_like_job_url = any(token in f"{domain}{path}" for token in ("job", "jobs", "career", "careers", "hiring", "position"))
    looks_like_job_title = any(keyword in title_text for keyword in JOB_SIGNAL_TITLE_KEYWORDS)
    if domain in JOB_DOMAINS or looks_like_job_url or has_job_signal or content_kind == "job_market" or looks_like_job_title:
        return DataCategory.JOB_POSTINGS

    if domain in BLOG_DOMAINS or "blog" in domain or path.startswith("/blog") or "/blog/" in path:
        return DataCategory.BLOGS

    looks_like_news_url = "news" in domain or path.startswith("/news") or "/news/" in path
    looks_like_news_text = content_kind == "news" or any(keyword in title_text or keyword in content_text for keyword in NEWS_SIGNAL_KEYWORDS)
    if domain in NEWS_DOMAINS or looks_like_news_url or looks_like_news_text:
        return DataCategory.NEWS

    return DataCategory.BLOGS


def build_topic_metadata(link: str, content: Any, topic_query: str, seed_tags: list[str] | None = None) -> dict[str, Any]:
    text = content_to_text(content).lower()

    ai_keyword_hits: set[str] = set()
    topic_scores: dict[str, int] = {}
    for topic, keywords in AI_TOPIC_KEYWORDS.items():
        hits = [keyword for keyword in keywords if keyword in text]
        if hits:
            topic_scores[topic] = len(hits)
            ai_keyword_hits.update(hits)

    topic_keywords = build_topic_keywords(topic_query)
    topic_hits = {keyword for keyword in topic_keywords if keyword in text}
    all_hits = ai_keyword_hits.union(topic_hits)

    ai_score = min(1.0, len(ai_keyword_hits) / 10)
    topic_score = min(1.0, len(topic_hits) / max(1, min(8, len(topic_keywords))))
    combined_score = round(max(ai_score, topic_score, min(1.0, len(all_hits) / 12)), 4)
    curated_links = {item for values in PREDEFINED_SOURCE_LINKS.values() for item in values}
    is_ai_related = bool(all_hits) or link in curated_links

    job_roles = sorted({role for role in JOB_ROLE_KEYWORDS if role in text})
    job_market_hits = sorted({keyword for keyword in JOB_MARKET_KEYWORDS if keyword in text})
    has_job_signal = bool(job_roles or job_market_hits)
    hiring_signal_score = round(min(1.0, (len(job_roles) + len(job_market_hits)) / 8), 4)

    content_kind = infer_content_kind(link, text)
    if has_job_signal and content_kind == "general":
        content_kind = "job_market"

    return {
        "source_domain": urlparse(link).netloc.lower(),
        "topic_query": topic_query,
        "content_kind": content_kind,
        "is_ai_related": is_ai_related,
        "ai_relevance_score": combined_score,
        "ai_topics": sorted(topic_scores.keys(), key=lambda topic: topic_scores[topic], reverse=True),
        "ai_keywords": sorted(all_hits),
        "has_job_signal": has_job_signal,
        "job_roles": job_roles,
        "hiring_signal_score": hiring_signal_score,
        "tags": normalize_tags(seed_tags or [], topic_hits, ai_keyword_hits, job_roles, job_market_hits),
    }


def enrich_document(document: SourceDocument) -> SourceDocument:
    metadata = build_topic_metadata(
        link=document.link,
        content=document.content,
        topic_query=document.topic_query,
        seed_tags=document.tags,
    )
    document.content = safe_truncate(document.content, settings.MAX_CONTENT_CHARS)
    document.summary = summarize_text(document.content)
    document.source_domain = metadata["source_domain"]
    document.content_kind = metadata["content_kind"]
    document.is_ai_related = metadata["is_ai_related"]
    document.ai_relevance_score = metadata["ai_relevance_score"]
    document.ai_topics = metadata["ai_topics"]
    document.ai_keywords = metadata["ai_keywords"]
    document.has_job_signal = metadata["has_job_signal"]
    document.job_roles = metadata["job_roles"]
    document.hiring_signal_score = metadata["hiring_signal_score"]
    document.tags = metadata["tags"]
    document.collection_name = infer_source_category(
        link=document.link,
        title=document.title,
        content=document.content,
        content_kind=document.content_kind,
        has_job_signal=document.has_job_signal,
    )
    return document
