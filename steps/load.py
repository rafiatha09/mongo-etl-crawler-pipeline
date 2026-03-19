from __future__ import annotations

from typing import Any

from domain.models import CrawlResult
from networks.mongo import MongoWarehouse


def load_results(warehouse: MongoWarehouse, results: list[CrawlResult], debug: bool = False) -> dict[str, Any]:
    summary = {
        "saved_count": 0,
        "moved_count": 0,
        "duplicate_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "per_collection": {},
    }

    for result in results:
        if result.status == "error":
            summary["error_count"] += 1
            _debug(debug, f"Skipping errored link: {result.link} | reason={result.reason}")
            continue

        if result.status in {"filtered_non_ai", "filtered_date_range"} or result.document is None:
            summary["skipped_count"] += 1
            _debug(debug, f"Filtered link: {result.link} | status={result.status} reason={result.reason}")
            continue

        existing_collection = warehouse.find_source_collection(result.document.link)
        if existing_collection == result.document.collection_name:
            summary["duplicate_count"] += 1
            _debug(debug, f"DUPLICATE -> {result.document.collection_name.value} | {result.link}")
            continue
        if existing_collection is not None and existing_collection != result.document.collection_name:
            warehouse.delete_source(existing_collection, result.document.link)
            warehouse.insert_document(result.document)
            collection_name = result.document.collection_name.value
            summary["per_collection"][collection_name] = summary["per_collection"].get(collection_name, 0) + 1
            summary["moved_count"] += 1
            _debug(
                debug,
                f"MOVED -> {result.link} | from={existing_collection.value} to={result.document.collection_name.value}",
            )
            continue

        operation = warehouse.insert_document(result.document)
        collection_name = result.document.collection_name.value
        summary["per_collection"][collection_name] = summary["per_collection"].get(collection_name, 0) + 1
        _debug(debug, f"{operation.upper()} -> {collection_name} | {result.link}")

        if operation == "inserted":
            summary["saved_count"] += 1

    return summary


def _debug(enabled: bool, message: str) -> None:
    if enabled:
        print(f"[DEBUG][load] {message}")
