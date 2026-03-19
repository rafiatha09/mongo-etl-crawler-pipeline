from __future__ import annotations

from urllib.parse import urlparse

from domain.exceptions import ImproperlyConfigured


def split_user_full_name(user_full_name: str | None) -> tuple[str, str]:
    if user_full_name is None:
        raise ImproperlyConfigured("User name is empty.")

    name_tokens = [token for token in user_full_name.split(" ") if token]
    if not name_tokens:
        raise ImproperlyConfigured("User name is empty.")
    if len(name_tokens) == 1:
        return name_tokens[0], name_tokens[0]

    return " ".join(name_tokens[:-1]), name_tokens[-1]


def safe_truncate(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 15].rstrip() + "\n...[truncated]"


def source_domain(link: str) -> str:
    return urlparse(link).netloc.lower()
