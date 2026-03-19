from __future__ import annotations

from datetime import date, datetime

from networks.crawlers import CrawlerDispatcher
from domain.models import CrawlResult, UserRecord


def crawl_links(
    links: list[str],
    user: UserRecord,
    topic_query: str,
    start_date: date | None = None,
    end_date: date | None = None,
    debug: bool = False,
) -> list[CrawlResult]:
    # The dispatcher keeps crawler-specific logic out of the ETL loop.
    # Each link is routed to the crawler that knows that source best.
    dispatcher = CrawlerDispatcher()
    results: list[CrawlResult] = []
    for index, link in enumerate(links, start=1):
        crawler = dispatcher.get_crawler(link)
        _debug(debug, f"[{index}/{len(links)}] Crawling {link} with {crawler.__class__.__name__}.")
        result = crawler.extract(link=link, user=user, topic_query=topic_query)
        # Date filtering happens after extraction because some sources only
        # expose publish dates once the page content has been parsed.
        result = _apply_date_filter(result=result, start_date=start_date, end_date=end_date, debug=debug)
        _debug(
            debug,
            f"[{index}/{len(links)}] Result status={result.status} collection={result.collection_name.value if result.collection_name else 'n/a'}.",
        )
        results.append(result)
    return results


def build_domain_metrics(results: list[CrawlResult]) -> dict[str, dict[str, float | int]]:
    # These metrics are saved with the ETL run so we can quickly inspect
    # where useful documents came from without querying raw collections.
    metrics: dict[str, dict[str, float | int]] = {}
    for result in results:
        domain = result.metadata.get("source_domain", "unknown") if result.metadata else "unknown"
        bucket = metrics.setdefault(
            domain,
            {
                "total": 0,
                "saved": 0,
                "filtered_non_ai": 0,
                "errors": 0,
                "ai_score_sum": 0.0,
            },
        )
        bucket["total"] += 1
        if result.status == "ready":
            bucket["saved"] += 1
        elif result.status == "filtered_non_ai":
            bucket["filtered_non_ai"] += 1
        elif result.status == "error":
            bucket["errors"] += 1

        if result.metadata:
            bucket["ai_score_sum"] += float(result.metadata.get("ai_relevance_score", 0.0))

    for domain, bucket in metrics.items():
        total = int(bucket["total"])
        score_sum = float(bucket.pop("ai_score_sum"))
        bucket["avg_ai_relevance_score"] = round(score_sum / total, 4) if total else 0.0
        metrics[domain] = bucket
    return metrics


def _apply_date_filter(
    result: CrawlResult,
    start_date: date | None,
    end_date: date | None,
    debug: bool,
) -> CrawlResult:
    if result.document is None:
        return result

    published_at = result.document.published_at
    if published_at is None:
        # Missing dates are common for GitHub and some web pages, so the
        # pipeline keeps them rather than throwing away potentially useful data.
        _debug(debug, f"No published_at found for {result.link}; keeping source.")
        return result

    published_date = _to_date(published_at)
    if start_date and published_date < start_date:
        _debug(debug, f"Filtered by date range: {result.link} published_at={published_date.isoformat()} before start_date.")
        result.status = "filtered_date_range"
        result.reason = "published_before_start_date"
        result.document = None
        return result

    if end_date and published_date > end_date:
        _debug(debug, f"Filtered by date range: {result.link} published_at={published_date.isoformat()} after end_date.")
        result.status = "filtered_date_range"
        result.reason = "published_after_end_date"
        result.document = None
        return result

    return result


def _to_date(value: datetime) -> date:
    return value.date()


def _debug(enabled: bool, message: str) -> None:
    if enabled:
        print(f"[DEBUG][transform] {message}")
