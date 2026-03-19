from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from domain.models import ETLRunSummary
from networks.mongo import MongoWarehouse
from steps.extract import get_or_create_user, resolve_links
from steps.load import load_results
from steps.transform import build_domain_metrics, crawl_links


def run_market_intelligence_etl(
    user_full_name: str,
    topic_query: str,
    links: list[str] | None = None,
    max_links: int | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    debug: bool = False,
) -> dict[str, Any]:
    _debug(debug, "Starting ETL pipeline.")
    warehouse = MongoWarehouse()
    _debug(debug, "MongoDB connection created.")
    warehouse.ensure_indexes()
    _debug(debug, "MongoDB indexes ensured.")

    started_at = datetime.now(timezone.utc)
    _debug(
        debug,
        f"Applying date filter start_date={start_date.isoformat() if start_date else 'none'} "
        f"end_date={end_date.isoformat() if end_date else 'none'}.",
    )
    user = get_or_create_user(warehouse, user_full_name=user_full_name, debug=debug)
    resolved_links = resolve_links(topic_query=topic_query, links=links, max_links=max_links, debug=debug)
    crawl_results = crawl_links(
        links=resolved_links,
        user=user,
        topic_query=topic_query,
        start_date=start_date,
        end_date=end_date,
        debug=debug,
    )
    load_summary = load_results(warehouse=warehouse, results=crawl_results, debug=debug)
    domain_metrics = build_domain_metrics(crawl_results)
    _debug(debug, f"Domain metrics built for {len(domain_metrics)} source domains.")
    analytics_pipeline, analytics_preview = warehouse.preview_topic_analytics(topic_query=topic_query)
    _debug(debug, f"MongoDB analytics preview generated with {len(analytics_preview)} row(s).")

    summary = ETLRunSummary(
        topic_query=topic_query,
        user_id=user.id,
        user_full_name=user.full_name,
        start_date=start_date or date.today(),
        end_date=end_date or date.today(),
        discovered_links=resolved_links,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        saved_count=load_summary["saved_count"],
        moved_count=load_summary["moved_count"],
        duplicate_count=load_summary["duplicate_count"],
        skipped_count=load_summary["skipped_count"],
        error_count=load_summary["error_count"],
        per_collection=load_summary["per_collection"],
        per_domain=domain_metrics,
        analytics_pipeline=analytics_pipeline,
        analytics_preview=analytics_preview,
    )
    warehouse.save_run_summary(summary)
    _debug(
        debug,
        "ETL run saved. "
        f"saved={summary.saved_count} moved={summary.moved_count} duplicates={summary.duplicate_count} "
        f"skipped={summary.skipped_count} errors={summary.error_count}",
    )
    return summary.model_dump(mode="json")


def _debug(enabled: bool, message: str) -> None:
    if enabled:
        print(f"[DEBUG][pipeline] {message}")
