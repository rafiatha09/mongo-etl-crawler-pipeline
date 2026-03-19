from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

from settings import settings
from steps.pipeline import run_market_intelligence_etl


def _default_start_date() -> str:
    return (date.today() - timedelta(days=30)).isoformat()


def _default_end_date() -> str:
    return date.today().isoformat()


def _parse_cli_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the AI Market Intelligence MongoDB ETL pipeline.")
    parser.add_argument("--user", default=settings.DEFAULT_USER_FULL_NAME, help="Name of the user running the ETL.")
    parser.add_argument("--topic", default=settings.DEFAULT_TOPIC_QUERY, help="Topic to crawl and enrich.")
    parser.add_argument("--link", action="append", dest="links", default=[], help="Manual source link.")
    parser.add_argument("--links-file", help="Optional text file with one link per line.")
    parser.add_argument(
        "--max-links",
        type=int,
        default=settings.DISCOVERY_MAX_LINKS,
        help="Target auto-discovered links. Auto mode keeps a minimum per-category quota.",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_cli_date,
        default=_parse_cli_date(_default_start_date()),
        help="Start date filter in YYYY-MM-DD format. Default is 30 days ago.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_cli_date,
        default=_parse_cli_date(_default_end_date()),
        help="End date filter in YYYY-MM-DD format. Default is today.",
    )
    parser.add_argument("--debug", action="store_true", help="Print step-by-step ETL debug logs.")
    return parser


def load_links(cli_links: list[str], links_file: str | None) -> list[str]:
    links = [link.strip() for link in cli_links if link.strip()]
    if not links_file:
        return links

    path = Path(links_file)
    if not path.exists():
        raise FileNotFoundError(f"Links file not found: {links_file}")

    file_links = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return links + file_links


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.start_date > args.end_date:
        parser.error("--start-date cannot be after --end-date.")

    links = load_links(args.links, args.links_file)

    if args.debug:
        print("[DEBUG] CLI arguments parsed successfully.")
        print(f"[DEBUG] user={args.user}")
        print(f"[DEBUG] topic={args.topic}")
        print(f"[DEBUG] manual_links_count={len(links)}")
        print(f"[DEBUG] max_links={args.max_links}")
        print(f"[DEBUG] start_date={args.start_date.isoformat()}")
        print(f"[DEBUG] end_date={args.end_date.isoformat()}")

    summary = run_market_intelligence_etl(
        user_full_name=args.user,
        topic_query=args.topic,
        links=links or None,
        max_links=args.max_links,
        start_date=args.start_date,
        end_date=args.end_date,
        debug=args.debug,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
