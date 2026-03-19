from __future__ import annotations

from networks.mongo import MongoWarehouse
from preprocessing.source_discovery import discover_links
from settings import settings


def get_or_create_user(warehouse: MongoWarehouse, user_full_name: str, debug: bool = False):
    _debug(debug, f"Looking up user '{user_full_name}'.")
    user = warehouse.get_or_create_user(user_full_name)
    _debug(debug, f"Using user id={user.id}.")
    return user


def resolve_links(
    topic_query: str,
    links: list[str] | None = None,
    max_links: int | None = None,
    debug: bool = False,
) -> list[str]:
    provided_links = [link.strip() for link in (links or []) if link and link.strip()]
    if provided_links:
        deduped = _dedupe_links(provided_links)
        _debug(debug, f"Using {len(deduped)} manually provided link(s).")
        return deduped

    _debug(debug, f"Auto-discovering links for topic='{topic_query}' with max_links={max_links or settings.DISCOVERY_MAX_LINKS}.")
    discovered_links = discover_links(topic_query=topic_query, max_links=max_links or settings.DISCOVERY_MAX_LINKS)
    deduped = _dedupe_links(discovered_links)
    _debug(debug, f"Discovered {len(deduped)} unique link(s).")
    if debug:
        for index, link in enumerate(deduped, start=1):
            _debug(debug, f"discovered_link[{index}]={link}")
    return deduped


def _dedupe_links(links: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append(link)
    return deduped


def _debug(enabled: bool, message: str) -> None:
    if enabled:
        print(f"[DEBUG][extract] {message}")
