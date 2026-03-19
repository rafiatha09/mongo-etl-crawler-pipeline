from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from email.utils import parsedate_to_datetime
import logging
import os
import shutil
import subprocess
import tempfile
import time
from typing import Any
from tempfile import mkdtemp
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

try:
    from langchain_community.document_loaders import AsyncHtmlLoader
    from langchain_community.document_transformers.html2text import Html2TextTransformer
except ImportError:  # pragma: no cover - optional runtime dependency
    AsyncHtmlLoader = None
    Html2TextTransformer = None

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except ImportError:  # pragma: no cover - optional runtime dependency
    webdriver = None
    Options = Any  # type: ignore[assignment]

try:
    import chromedriver_autoinstaller
except ImportError:  # pragma: no cover - optional runtime dependency
    chromedriver_autoinstaller = None

from domain.categories import DataCategory
from domain.constants import JOB_DOMAINS, NEWS_DOMAINS, RESEARCH_DOMAINS, BLOG_DOMAINS
from domain.models import CrawlResult, SourceDocument, UserRecord
from preprocessing.enrichment import enrich_document
from preprocessing.source_discovery import get_cached_medium_entry
from preprocessing.utils import source_domain
from settings import settings

logger = logging.getLogger(__name__)

if chromedriver_autoinstaller is not None:
    try:
        chromedriver_autoinstaller.install()
    except Exception:
        pass

BINARY_FILE_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".pdf",
    ".zip",
    ".tar",
    ".gz",
    ".7z",
    ".jar",
    ".bin",
    ".exe",
    ".so",
    ".dylib",
    ".dll",
    ".mp3",
    ".mp4",
    ".mov",
    ".avi",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
    ".eot",
    ".pyc",
    ".class",
}


class BaseCrawler(ABC):
    @abstractmethod
    def extract(self, link: str, user: UserRecord, topic_query: str) -> CrawlResult:
        raise NotImplementedError

    def _fetch_html(self, link: str) -> str:
        response = requests.get(
            link,
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": settings.USER_AGENT, **self._extra_headers()},
        )
        response.raise_for_status()
        return response.text

    def _extra_headers(self) -> dict[str, str]:
        return {}


class BaseSeleniumCrawler(BaseCrawler, ABC):
    def __init__(self, scroll_limit: int = 5) -> None:
        if webdriver is None:
            raise RuntimeError("selenium is not installed. Install requirements.txt to use Selenium-based crawlers.")

        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--headless=new")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--log-level=3")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-background-networking")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument(f"--user-data-dir={mkdtemp()}")
        options.add_argument(f"--data-path={mkdtemp()}")
        options.add_argument(f"--disk-cache-dir={mkdtemp()}")
        options.add_argument("--remote-debugging-port=9226")

        self.set_extra_driver_options(options)
        self.scroll_limit = scroll_limit
        self.driver = webdriver.Chrome(options=options)

    def set_extra_driver_options(self, options: Options) -> None:
        _ = options

    def scroll_page(self) -> None:
        current_scroll = 0
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height or (self.scroll_limit and current_scroll >= self.scroll_limit):
                break
            last_height = new_height
            current_scroll += 1


class CustomArticleCrawler(BaseCrawler):
    def extract(self, link: str, user: UserRecord, topic_query: str) -> CrawlResult:
        logger.info("Starting scraping article: %s", link)
        try:
            if AsyncHtmlLoader is None or Html2TextTransformer is None:
                raise RuntimeError("langchain-community is not available")

            loader = AsyncHtmlLoader([link])
            docs = loader.load()
            doc = docs[0]
            try:
                html2text = Html2TextTransformer()
                docs_transformed = html2text.transform_documents(docs)
                doc_transformed = docs_transformed[0]
                parsed_content = doc_transformed.page_content
                metadata = doc_transformed.metadata
            except Exception as exc:
                logger.warning(
                    "Html2TextTransformer failed (%s). Falling back to BeautifulSoup text extraction for: %s",
                    exc,
                    link,
                )
                parsed_content = BeautifulSoup(doc.page_content or "", "html.parser").get_text(separator="\n")
                metadata = doc.metadata
        except Exception:
            try:
                html = self._fetch_html(link)
            except Exception as exc:
                return CrawlResult(status="error", link=link, reason=str(exc))

            soup = BeautifulSoup(html, "html.parser")
            if _is_security_verification_page(soup.get_text(" ", strip=True)):
                return CrawlResult(status="error", link=link, reason="bot_protection_challenge")
            title = _extract_title(soup) or link
            description = _extract_description(soup)
            published_at = _extract_published_at(soup)
            text_content = _extract_text_content(soup)
            tags = _extract_tags(soup)
            content = "\n\n".join(part for part in [description, text_content] if part)
            return _build_document_result(
                link=link,
                user=user,
                topic_query=topic_query,
                collection_name=_infer_collection_name(link),
                title=title,
                content=content,
                published_at=published_at,
                tags=tags,
                platform=source_domain(link),
                source=source_domain(link),
                raw_metadata={
                    "description": description,
                    "html_title": soup.title.string.strip() if soup.title and soup.title.string else None,
                },
            )

        title = str(metadata.get("title") or link)
        description = metadata.get("description")
        published_at = _parse_datetime(str(metadata.get("published_time"))) if metadata.get("published_time") else None
        content = "\n\n".join(part for part in [description, parsed_content] if part)
        if _is_security_verification_page(content):
            return CrawlResult(status="error", link=link, reason="bot_protection_challenge")
        result = _build_document_result(
            link=link,
            user=user,
            topic_query=topic_query,
            collection_name=_infer_collection_name(link),
            title=title,
            content=content,
            published_at=published_at,
            tags=[],
            platform="website",
            source=source_domain(link),
            raw_metadata=metadata,
        )
        logger.info("Finished scraping custom article: %s", link)
        return result


class GenericWebCrawler(CustomArticleCrawler):
    pass


class MediumCrawler(BaseSeleniumCrawler):
    def set_extra_driver_options(self, options: Options) -> None:
        options.add_argument("--disable-blink-features=AutomationControlled")

    def extract(self, link: str, user: UserRecord, topic_query: str) -> CrawlResult:
        cached_entry = get_cached_medium_entry(link)
        if cached_entry is not None:
            logger.info("Using Medium RSS content for: %s", link)
            content = "\n\n".join(
                part
                for part in [
                    cached_entry.get("description"),
                    cached_entry.get("content"),
                ]
                if isinstance(part, str) and part.strip()
            )
            published_at = _parse_datetime(cached_entry.get("published_at"))
            return _build_document_result(
                link=link,
                user=user,
                topic_query=topic_query,
                collection_name=DataCategory.BLOGS,
                title=str(cached_entry.get("title") or link),
                content=content or str(cached_entry.get("description") or ""),
                published_at=published_at,
                tags=[],
                platform="medium",
                source="medium",
                raw_metadata={"feed_url": cached_entry.get("feed_url"), "ingestion_mode": "rss"},
            )

        try:
            self.driver.get(link)
            self.scroll_page()
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
        except Exception as exc:
            return CrawlResult(status="error", link=link, reason=str(exc))
        finally:
            try:
                self.driver.quit()
            except Exception:
                pass

        title = _extract_text_from_selectors(soup, ("h1",)) or _extract_title(soup) or link
        subtitle = _extract_text_from_selectors(soup, ("h2",))
        content = "\n\n".join(
            part
            for part in [
                subtitle,
                soup.get_text("\n", strip=True),
            ]
            if part
        )
        if _is_security_verification_page(content):
            return CrawlResult(
                status="error",
                link=link,
                reason="bot_protection_challenge: Medium blocked the page. Use predefined auto-discovery or a feed-backed Medium URL.",
            )
        tags = _extract_tags(soup)
        return _build_document_result(
            link=link,
            user=user,
            topic_query=topic_query,
            collection_name=DataCategory.BLOGS,
            title=title,
            content=content,
            published_at=_extract_published_at(soup),
            tags=tags,
            platform="medium",
            source="medium",
            raw_metadata={"subtitle": subtitle},
        )


class GitHubCrawler(BaseCrawler):
    def _extra_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if settings.GITHUB_API_TOKEN:
            headers["Authorization"] = f"Bearer {settings.GITHUB_API_TOKEN}"
        return headers

    def extract(self, link: str, user: UserRecord, topic_query: str) -> CrawlResult:
        repo_name = link.rstrip("/").split("/")[-1]
        local_temp = tempfile.mkdtemp()
        original_cwd = os.getcwd()

        try:
            os.chdir(local_temp)
            clone_process = subprocess.run(["git", "clone", "--depth", "1", link], capture_output=True, text=True)
            if clone_process.returncode != 0:
                raise RuntimeError(f"Failed to clone repository '{link}'. stderr: {clone_process.stderr.strip()}")

            cloned_repos = os.listdir(local_temp)
            if not cloned_repos:
                raise RuntimeError(f"No repository was created after cloning '{link}'.")

            repo_path = os.path.join(local_temp, cloned_repos[0])
            readme_path, content = _extract_repository_readme(repo_path)
            if not content:
                raise RuntimeError(f"Repository '{link}' does not contain a readable README file.")

            return _build_document_result(
                link=link,
                user=user,
                topic_query=topic_query,
                collection_name=DataCategory.GITHUB,
                title=repo_name,
                content=content,
                published_at=None,
                tags=["readme", repo_name.lower()][: settings.MAX_TAGS],
                platform="github",
                source="github",
                raw_metadata={"readme_path": readme_path, "ingestion_mode": "readme_only"},
            )
        except Exception as exc:
            return CrawlResult(status="error", link=link, reason=str(exc))
        finally:
            os.chdir(original_cwd)
            shutil.rmtree(local_temp, ignore_errors=True)


class LinkedInJobCrawler(BaseCrawler):
    def extract(self, link: str, user: UserRecord, topic_query: str) -> CrawlResult:
        try:
            html = self._fetch_html(link)
        except Exception as exc:
            return CrawlResult(status="error", link=link, reason=str(exc))

        soup = BeautifulSoup(html, "html.parser")
        title = (
            _extract_text_from_selectors(
                soup,
                (
                    "h1.top-card-layout__title",
                    "h1.topcard__title",
                    "h1",
                ),
            )
            or _extract_title(soup)
            or link
        )
        company = _extract_text_from_selectors(
            soup,
            (
                "a.topcard__org-name-link",
                "span.topcard__flavor",
                "div.topcard__flavor-row span",
            ),
        )
        location = _extract_text_from_selectors(
            soup,
            (
                "span.topcard__flavor--bullet",
                "span.topcard__flavor.topcard__flavor--bullet",
            ),
        )
        description = _extract_text_from_selectors(
            soup,
            (
                "div.show-more-less-html__markup",
                "section.show-more-less-html",
                "div.description__text",
            ),
        )
        criteria = _extract_joined_text_from_selectors(
            soup,
            (
                "li.description__job-criteria-item",
                "span.description__job-criteria-text",
            ),
        )
        published_at = _extract_published_at(soup)
        content_parts = [
            f"Job title: {title}",
            f"Company: {company}" if company else "",
            f"Location: {location}" if location else "",
            criteria,
            description,
        ]
        content = _normalize_content("\n\n".join(part for part in content_parts if part))
        if len(content) < settings.MIN_EXTRACTED_CONTENT_CHARS:
            return CrawlResult(
                status="error",
                link=link,
                reason=f"insufficient_content: extracted {len(content)} chars",
            )

        document = SourceDocument(
            collection_name=DataCategory.JOB_POSTINGS,
            title=title,
            content=content,
            summary=company,
            published_at=published_at,
            source="linkedin",
            source_domain=source_domain(link),
            link=link,
            platform="linkedin",
            topic_query=topic_query,
            tags=[tag for tag in [company, location] if tag],
            created_by_user_id=user.id,
            created_by_user_name=user.full_name,
            raw_metadata={"company": company, "location": location},
        )
        document = enrich_document(document)

        return CrawlResult(
            status="ready",
            link=link,
            collection_name=document.collection_name,
            document=document,
            metadata=document.model_dump(mode="python"),
        )


class CrawlerDispatcher:
    def __init__(self) -> None:
        self._crawler_map = {
            "github.com": GitHubCrawler,
            "www.github.com": GitHubCrawler,
            "linkedin.com": LinkedInJobCrawler,
            "www.linkedin.com": LinkedInJobCrawler,
            "medium.com": MediumCrawler,
            "www.medium.com": MediumCrawler,
        }

    def get_crawler(self, link: str) -> BaseCrawler:
        domain = source_domain(link)
        crawler_class = self._crawler_map.get(domain, CustomArticleCrawler)
        return crawler_class()


def _infer_collection_name(link: str) -> DataCategory:
    domain = source_domain(link)
    if domain in {"github.com", "www.github.com"}:
        return DataCategory.GITHUB
    if domain in RESEARCH_DOMAINS or _looks_like_research_path(link):
        return DataCategory.RESEARCH_PAPERS
    if domain in JOB_DOMAINS or _looks_like_job_posting(link):
        return DataCategory.JOB_POSTINGS
    if domain in NEWS_DOMAINS or _looks_like_news_site(domain, link):
        return DataCategory.NEWS
    if domain in BLOG_DOMAINS or _looks_like_blog_site(domain, link):
        return DataCategory.BLOGS
    return DataCategory.BLOGS


def _extract_title(soup: BeautifulSoup) -> str | None:
    for selector in ("meta[property='og:title']", "meta[name='twitter:title']", "title", "h1"):
        tag = soup.select_one(selector)
        if tag is None:
            continue
        if tag.name == "meta":
            content = tag.get("content")
            if content:
                return content.strip()
        else:
            text = tag.get_text(" ", strip=True)
            if text:
                return text
    return None


def _extract_description(soup: BeautifulSoup) -> str | None:
    for selector in ("meta[name='description']", "meta[property='og:description']"):
        tag = soup.select_one(selector)
        if tag and tag.get("content"):
            return tag["content"].strip()
    return None


def _extract_text_content(soup: BeautifulSoup) -> str:
    for selector in ("article", "main", "[role='main']", "body"):
        tag = soup.select_one(selector)
        if tag:
            return tag.get_text("\n", strip=True)
    return soup.get_text("\n", strip=True)


def _extract_tags(soup: BeautifulSoup) -> list[str]:
    keywords = soup.select_one("meta[name='keywords']")
    if keywords and keywords.get("content"):
        return [value.strip().lower() for value in keywords["content"].split(",") if value.strip()]
    return []


def _extract_published_at(soup: BeautifulSoup) -> datetime | None:
    candidates: list[Any] = []
    for selector in (
        "meta[property='article:published_time']",
        "meta[name='article:published_time']",
        "meta[name='publish_date']",
        "time[datetime]",
    ):
        candidates.extend(soup.select(selector))

    for tag in candidates:
        raw_value = tag.get("content") or tag.get("datetime")
        parsed = _parse_datetime(raw_value)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return parsedate_to_datetime(raw_value)
    except (TypeError, ValueError):
        return None


def _extract_github_readme(soup: BeautifulSoup) -> str:
    for selector in ("article.markdown-body", "div.markdown-body", "#readme"):
        tag = soup.select_one(selector)
        if tag:
            return tag.get_text("\n", strip=True)
    return ""


def _extract_github_topics(soup: BeautifulSoup) -> list[str]:
    topics = []
    for tag in soup.select("a.topic-tag, a[data-view-component='true'][href*='/topics/']"):
        text = tag.get_text(" ", strip=True).lower()
        if text and text not in topics:
            topics.append(text)
    return topics


def _extract_repository_readme(repo_path: str) -> tuple[str | None, str]:
    preferred_names = (
        "README.md",
        "README.rst",
        "README.txt",
        "README",
        "readme.md",
        "readme.rst",
        "readme.txt",
        "readme",
    )

    for file_name in preferred_names:
        candidate_path = os.path.join(repo_path, file_name)
        if not os.path.isfile(candidate_path):
            continue
        content = _read_text_file(candidate_path)
        if content and content.strip():
            return file_name, content

    for root, _, files in os.walk(repo_path):
        if ".git" in root.split(os.sep):
            continue
        for file_name in files:
            lower_name = file_name.lower()
            if not lower_name.startswith("readme"):
                continue
            candidate_path = os.path.join(root, file_name)
            content = _read_text_file(candidate_path)
            if content and content.strip():
                relative_path = os.path.relpath(candidate_path, repo_path)
                return relative_path, content

    return None, ""


def _build_repository_tree(repo_path: str, ignore: tuple[str, ...]) -> tuple[dict[str, str], bool]:
    max_files = max(1, settings.GITHUB_MAX_FILES)
    max_file_chars = max(200, settings.GITHUB_MAX_FILE_CHARS)
    max_total_chars = max(2000, settings.GITHUB_MAX_TOTAL_CHARS)
    skip_file_bytes = max(1024, settings.GITHUB_SKIP_FILE_BYTES)

    tree: dict[str, str] = {}
    total_chars = 0
    trimmed = False

    for root, _, files in os.walk(repo_path):
        directory = root.replace(repo_path, "").lstrip("/")
        if directory.startswith(ignore):
            continue

        for file_name in files:
            relative_path = os.path.join(directory, file_name) if directory else file_name
            if _should_skip_repo_file(relative_path, file_name, ignore):
                continue

            full_path = os.path.join(root, file_name)
            try:
                file_size = os.path.getsize(full_path)
            except OSError:
                continue

            if file_size > skip_file_bytes:
                trimmed = True
                continue

            text = _read_text_file(full_path)
            if text is None or not text.strip():
                continue

            if len(text) > max_file_chars:
                text = text[:max_file_chars] + "\n...[truncated]"
                trimmed = True

            projected_chars = total_chars + len(relative_path) + len(text)
            if projected_chars > max_total_chars:
                trimmed = True
                continue

            tree[relative_path] = text
            total_chars = projected_chars

            if len(tree) >= max_files or total_chars >= max_total_chars:
                trimmed = True
                break

        if len(tree) >= max_files or total_chars >= max_total_chars:
            break

    if not tree:
        tree["README"] = "No text content extracted from repository after ETL size and file filters."

    return tree, trimmed


def _should_skip_repo_file(relative_path: str, file_name: str, ignore: tuple[str, ...]) -> bool:
    lower_name = file_name.lower()
    lower_path = relative_path.lower()

    if lower_name.endswith(ignore) or lower_path.startswith(ignore):
        return True
    if any(part.startswith(".git") for part in lower_path.split(os.sep)):
        return True
    if os.path.splitext(lower_name)[1] in BINARY_FILE_EXTENSIONS:
        return True

    return False


def _read_text_file(path: str) -> str | None:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as file_obj:
            return file_obj.read()
    except Exception:
        return None


def _repository_tree_to_text(tree: dict[str, str]) -> str:
    parts = []
    for path, content in tree.items():
        parts.append(f"FILE: {path}\n{content}")
    return "\n\n".join(parts)


def _build_document_result(
    *,
    link: str,
    user: UserRecord,
    topic_query: str,
    collection_name: DataCategory,
    title: str,
    content: str,
    published_at: datetime | None,
    tags: list[str],
    platform: str,
    source: str,
    raw_metadata: dict[str, Any],
) -> CrawlResult:
    normalized_content = _normalize_content(content)
    if len(normalized_content) < settings.MIN_EXTRACTED_CONTENT_CHARS:
        return CrawlResult(
            status="error",
            link=link,
            reason=f"insufficient_content: extracted {len(normalized_content)} chars",
        )

    document = SourceDocument(
        collection_name=collection_name,
        title=title,
        content=normalized_content,
        published_at=published_at,
        source=source,
        source_domain=source_domain(link),
        link=link,
        platform=platform,
        topic_query=topic_query,
        tags=tags,
        created_by_user_id=user.id,
        created_by_user_name=user.full_name,
        raw_metadata=raw_metadata,
    )
    document = enrich_document(document)

    return CrawlResult(
        status="ready",
        link=link,
        collection_name=document.collection_name,
        document=document,
        metadata=document.model_dump(mode="python"),
    )


def _extract_text_from_selectors(soup: BeautifulSoup, selectors: tuple[str, ...]) -> str | None:
    for selector in selectors:
        tag = soup.select_one(selector)
        if tag:
            text = tag.get_text(" ", strip=True)
            if text:
                return text
    return None


def _extract_joined_text_from_selectors(soup: BeautifulSoup, selectors: tuple[str, ...]) -> str:
    parts: list[str] = []
    for selector in selectors:
        for tag in soup.select(selector):
            text = tag.get_text(" ", strip=True)
            if text and text not in parts:
                parts.append(text)
    return "\n".join(parts)


def _normalize_content(content: str) -> str:
    return "\n".join(line.strip() for line in content.splitlines() if line.strip()).strip()


def _looks_like_research_path(link: str) -> bool:
    parsed = urlparse(link)
    path = parsed.path.lower()
    return any(token in path for token in ("/abs/", "/pdf/", "/paper", "/papers", "/research"))


def _looks_like_job_posting(link: str) -> bool:
    parsed = urlparse(link)
    path = parsed.path.lower()
    host = parsed.netloc.lower()
    combined = f"{host}{path}"
    return any(token in combined for token in ("job", "jobs", "careers", "career", "hiring", "positions"))


def _looks_like_news_site(domain: str, link: str) -> bool:
    path = urlparse(link).path.lower()
    return "news" in domain or path.startswith("/news") or "/news/" in path


def _looks_like_blog_site(domain: str, link: str) -> bool:
    path = urlparse(link).path.lower()
    return "blog" in domain or path.startswith("/blog") or "/blog/" in path


def _is_security_verification_page(text: str) -> bool:
    normalized = text.lower()
    return (
        "performing security verification" in normalized
        or "enable javascript and cookies to continue" in normalized
        or "this website uses a security service to protect against malicious bots" in normalized
        or "verification successful. waiting for" in normalized
        or "ray id" in normalized and "cloudflare" in normalized
    )
