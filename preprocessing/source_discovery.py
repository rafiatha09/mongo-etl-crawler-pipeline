from __future__ import annotations

import math
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from domain.constants import PREDEFINED_SOURCE_LINKS
from settings import settings

# Medium discovery is feed-backed so we can reuse article content later
# without always opening Medium pages directly.
MEDIUM_RSS_CACHE: dict[str, dict[str, str | None]] = {}
CATEGORY_ORDER = ["blogs", "github", "job_postings", "news", "research_papers"]


def discover_links(topic_query: str, max_links: int | None = None) -> list[str]:
    topic = topic_query.strip() or settings.DEFAULT_TOPIC_QUERY
    # Auto-discovery tries to keep a balanced mix across the five portfolio
    # collections instead of returning a random pile of URLs.
    minimum_total = settings.DISCOVERY_MIN_PER_CATEGORY * len(CATEGORY_ORDER)
    target_count = max(max_links or settings.DISCOVERY_MAX_LINKS, minimum_total)
    category_targets = _build_category_targets(target_count)

    candidates: list[str] = []
    for category in CATEGORY_ORDER:
        candidates.extend(_discover_category_links(category=category, topic=topic, limit=category_targets[category]))

    deduped: list[str] = []
    seen: set[str] = set()
    for link in candidates:
        normalized = _normalize_link(link)
        if not _is_http_url(normalized) or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
        if len(deduped) >= target_count:
            break

    if deduped:
        return deduped

    return _load_local_fallback_links()[:target_count]


def _build_category_targets(target_count: int) -> dict[str, int]:
    # First reserve the minimum per category, then spread any extra slots
    # round-robin so one category does not starve the others.
    category_targets = {category: settings.DISCOVERY_MIN_PER_CATEGORY for category in CATEGORY_ORDER}
    extras = max(0, target_count - sum(category_targets.values()))
    for index in range(extras):
        category = CATEGORY_ORDER[index % len(CATEGORY_ORDER)]
        category_targets[category] += 1
    return category_targets


def _discover_category_links(category: str, topic: str, limit: int) -> list[str]:
    # Each category has a slightly different discovery strategy:
    # LinkedIn for jobs, topic pages for GitHub, arXiv listings for papers,
    # and curated host pages for blogs/news.
    if category == "job_postings":
        return _discover_job_links(topic, limit)
    if category == "github":
        return _discover_github_links(limit)
    if category == "research_papers":
        return _discover_research_links(limit)
    return _expand_predefined_source_links(category=category, limit=limit)


def _discover_job_links(topic: str, limit: int) -> list[str]:
    return _discover_linkedin_job_links(topic, limit)


def _discover_linkedin_job_links(topic: str, limit: int) -> list[str]:
    keywords = urllib.parse.quote_plus(topic)
    location = urllib.parse.quote_plus(settings.LINKEDIN_JOB_LOCATION)
    page_size = max(1, settings.LINKEDIN_JOB_PAGE_SIZE)
    max_pages = max(1, settings.LINKEDIN_JOB_MAX_PAGES)

    links: list[str] = []
    seen: set[str] = set()
    for page_index in range(max_pages):
        start = page_index * page_size
        url = (
            "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            f"?keywords={keywords}&location={location}&start={start}"
        )
        html = _fetch_text(url, accept="text/html")
        if html is None:
            continue

        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.select("a.base-card__full-link, a[href*='/jobs/view/']")
        for anchor in anchors:
            href = anchor.get("href")
            if not isinstance(href, str):
                continue
            normalized = _normalize_link(urljoin("https://www.linkedin.com", href.strip()))
            if not _is_http_url(normalized) or normalized in seen:
                continue
            seen.add(normalized)
            links.append(normalized)
            if len(links) >= limit:
                return links

    return links


def _fetch_text(url: str, accept: str = "text/plain") -> str | None:
    headers = {"User-Agent": settings.USER_AGENT, "Accept": accept}
    request = urllib.request.Request(url=url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=settings.DISCOVERY_TIMEOUT_SECONDS) as response:
            raw = response.read()
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return None


def _is_http_url(link: str) -> bool:
    parsed = urllib.parse.urlparse(link)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _expand_predefined_source_links(category: str, limit: int) -> list[str]:
    source_urls = list(PREDEFINED_SOURCE_LINKS.get(category, []))
    if not source_urls:
        return []

    # Spread the request across multiple host pages so we do not over-index
    # on the first source in the list.
    per_source = max(1, math.ceil(limit / len(source_urls)))
    links: list[str] = []
    for source_url in source_urls:
        links.extend(_discover_links_from_source(source_url, max(2, per_source)))
        if len(links) >= limit:
            break
    return links[:limit]


def _discover_github_links(limit: int) -> list[str]:
    source_urls = list(PREDEFINED_SOURCE_LINKS.get("github", []))
    if not source_urls:
        return []

    # GitHub discovery starts from topic pages and then filters down to
    # repository-looking URLs only.
    per_source = max(1, math.ceil(limit / len(source_urls)))
    links: list[str] = []
    seen: set[str] = set()
    for source_url in source_urls:
        html = _fetch_text(source_url, accept="text/html")
        if html is None:
            continue

        soup = BeautifulSoup(html, "html.parser")
        source_count = 0
        for anchor in soup.select("a[href]"):
            href = anchor.get("href")
            if not isinstance(href, str):
                continue

            normalized = _normalize_link(urljoin("https://github.com", href.strip()))
            if not _is_github_repository_link(normalized) or normalized in seen:
                continue

            seen.add(normalized)
            links.append(normalized)
            source_count += 1
            if len(links) >= limit:
                return links
            if source_count >= per_source:
                break
    return links[:limit]


def _discover_research_links(limit: int) -> list[str]:
    source_urls = list(PREDEFINED_SOURCE_LINKS.get("research_papers", []))
    if not source_urls:
        return []

    # Research discovery expands recent arXiv listing pages into `/abs/...`
    # links so the crawler later reads actual paper pages, not index pages.
    per_source = max(1, math.ceil(limit / len(source_urls)))
    links: list[str] = []
    seen: set[str] = set()
    for source_url in source_urls:
        html = _fetch_text(source_url, accept="text/html")
        if html is None:
            continue

        soup = BeautifulSoup(html, "html.parser")
        source_count = 0
        for anchor in soup.select("a[href]"):
            href = anchor.get("href")
            if not isinstance(href, str):
                continue

            normalized = _normalize_link(urljoin(source_url, href.strip()))
            if not _is_research_paper_link(normalized) or normalized in seen:
                continue

            seen.add(normalized)
            links.append(normalized)
            source_count += 1
            if len(links) >= limit:
                return links
            if source_count >= per_source:
                break
    return links[:limit]


def _discover_links_from_source(source_url: str, limit: int) -> list[str]:
    domain = urlparse(source_url).netloc.lower()
    if "medium.com" in domain:
        return _discover_medium_feed_links(source_url, limit)
    return _discover_same_domain_links(source_url, limit)


def _discover_medium_feed_links(source_url: str, limit: int) -> list[str]:
    # Medium host pages are unreliable for crawling, so discovery uses the
    # corresponding RSS feed to get article links instead.
    feed_url = _medium_feed_url(source_url)
    links: list[str] = []
    for entry in _load_medium_feed_entries(feed_url):
        normalized = entry["link"]
        if _is_http_url(normalized) and normalized not in links:
            links.append(normalized)
        if len(links) >= limit:
            break
    return links


def get_cached_medium_entry(link: str) -> dict[str, str | None] | None:
    # During crawling, Medium articles can reuse the feed payload gathered
    # during discovery instead of fetching the page again.
    normalized = _normalize_link(link)
    cached = MEDIUM_RSS_CACHE.get(normalized)
    if cached is not None:
        return cached

    feed_url = _medium_feed_url_for_article(link)
    if feed_url is None:
        return None

    for entry in _load_medium_feed_entries(feed_url):
        if entry["link"] == normalized:
            return entry
    return None


def _load_medium_feed_entries(feed_url: str) -> list[dict[str, str | None]]:
    text = _fetch_text(feed_url, accept="application/rss+xml, application/xml, text/xml")
    if text is None:
        return []

    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []

    entries: list[dict[str, str | None]] = []
    for item in root.findall(".//item"):
        link = item.findtext("link")
        if not isinstance(link, str):
            continue

        normalized = _normalize_link(link)
        if not _is_http_url(normalized):
            continue

        entry = {
            "link": normalized,
            "title": _clean_xml_text(item.findtext("title")),
            "description": _clean_html_text(item.findtext("description")),
            "content": _extract_medium_content(item),
            "published_at": _clean_xml_text(item.findtext("pubDate")),
            "feed_url": feed_url,
        }
        MEDIUM_RSS_CACHE[normalized] = entry
        entries.append(entry)
    return entries


def _medium_feed_url(source_url: str) -> str:
    parsed = urlsplit(source_url)
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    if host in {"medium.com", "www.medium.com"}:
        return urlunsplit((parsed.scheme, parsed.netloc, f"/feed{path}", "", ""))
    return urlunsplit((parsed.scheme, parsed.netloc, "/feed", "", ""))


def _medium_feed_url_for_article(article_url: str) -> str | None:
    parsed = urlsplit(article_url)
    host = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    if not path:
        return None

    if host in {"medium.com", "www.medium.com"}:
        first_segment = path.lstrip("/").split("/", 1)[0]
        if not first_segment:
            return None
        return urlunsplit((parsed.scheme, parsed.netloc, f"/feed/{first_segment}", "", ""))

    if host.endswith(".medium.com"):
        return urlunsplit((parsed.scheme, parsed.netloc, "/feed", "", ""))

    return None


def _discover_same_domain_links(source_url: str, limit: int) -> list[str]:
    html = _fetch_text(source_url, accept="text/html")
    if html is None:
        return []

    soup = BeautifulSoup(html, "html.parser")
    base_domain = urlparse(source_url).netloc.lower()
    discovered: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = anchor.get("href")
        if not isinstance(href, str):
            continue

        normalized = _normalize_link(urljoin(source_url, href.strip()))
        parsed = urlparse(normalized)
        if not _is_http_url(normalized):
            continue
        if parsed.netloc.lower() != base_domain:
            continue
        if not _is_likely_content_link(source_url, normalized):
            continue
        if normalized in seen:
            continue

        seen.add(normalized)
        discovered.append(normalized)
        if len(discovered) >= limit:
            break

    return discovered


def _is_likely_content_link(source_url: str, link: str) -> bool:
    source_path = urlparse(source_url).path.rstrip("/")
    path = urlparse(link).path.rstrip("/")
    if not path or path == source_path:
        return False

    blocked_tokens = {
        "/tag/",
        "/tags/",
        "/author/",
        "/authors/",
        "/about",
        "/privacy",
        "/subscribe",
        "/login",
        "/signin",
        "/search",
    }
    if any(token in path.lower() for token in blocked_tokens):
        return False

    if path.count("/") < 2:
        return False

    return True


def _normalize_link(link: str) -> str:
    stripped = link.strip()
    parsed = urlsplit(stripped)
    if not parsed.query:
        return stripped

    filtered_params = []
    for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower().startswith("utm_"):
            continue
        filtered_params.append((key, value))

    normalized_query = urllib.parse.urlencode(filtered_params)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, normalized_query, parsed.fragment))


def _is_github_repository_link(link: str) -> bool:
    parsed = urlparse(link)
    if parsed.netloc.lower() not in {"github.com", "www.github.com"}:
        return False

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 2:
        return False

    blocked_first_segments = {
        "topics",
        "orgs",
        "organizations",
        "explore",
        "resources",
        "articles",
        "blog",
        "marketplace",
        "settings",
        "login",
        "signup",
        "features",
        "collections",
        "trending",
        "readme",
        "about",
        "site",
        "enterprise",
        "customer-stories",
        "solutions",
        "events",
        "sponsors",
    }
    return parts[0].lower() not in blocked_first_segments


def _is_research_paper_link(link: str) -> bool:
    parsed = urlparse(link)
    if parsed.netloc.lower() not in {"arxiv.org", "www.arxiv.org"}:
        return False
    return parsed.path.startswith("/abs/")


def _extract_medium_content(item: ET.Element) -> str | None:
    namespace = "{http://purl.org/rss/1.0/modules/content/}"
    content_node = item.find(f"{namespace}encoded")
    if content_node is not None and content_node.text:
        parsed = _clean_html_text(content_node.text)
        if parsed:
            return parsed
    return _clean_html_text(item.findtext("description"))


def _clean_xml_text(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = " ".join(value.split()).strip()
    return parsed or None


def _clean_html_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = BeautifulSoup(value, "html.parser").get_text("\n", strip=True)
    parsed = "\n".join(line for line in (part.strip() for part in text.splitlines()) if line)
    return parsed or None


def _load_local_fallback_links() -> list[str]:
    fallback_path = Path("ai_links.txt.example")
    if not fallback_path.exists():
        return list(settings.DEFAULT_LINKS)

    links: list[str] = []
    for line in fallback_path.read_text(encoding="utf-8").splitlines():
        parsed = line.strip()
        if parsed and not parsed.startswith("#") and _is_http_url(parsed):
            links.append(parsed)
    return links
