# AI Market Intelligence ETL with MongoDB

This repository is an ETL pipeline for AI market intelligence. It crawls curated web sources, normalizes documents, enriches them with lightweight metadata, and stores the results in MongoDB for downstream RAG, analytics, or dashboards.

## Structure

```text
.
├── domain/          # enums, models, config, constants
├── networks/        # MongoDB access and crawler implementations
├── preprocessing/   # source discovery and enrichment
├── steps/           # extract / transform / load orchestration
├── main.py          # CLI entrypoint
├── settings.py      # settings export
├── requirements.txt
└── Dockerfile
```

## Collections

The ETL writes to these MongoDB collections:

- `users`
- `blogs`
- `news`
- `github`
- `research_papers`
- `job_postings`
- `etl_runs`

Each saved source document includes fields such as:

- `title`
- `content`
- `summary`
- `published_at`
- `source`
- `source_domain`
- `link`
- `topic_query`
- `tags`
- `content_kind`
- `is_ai_related`
- `ai_relevance_score`
- `ai_topics`
- `job_roles`

## Current Source Strategy

The pipeline separates sources into five domains:

- `blogs`
- `github`
- `job_postings`
- `news`
- `research_papers`

Auto-discovery is based on predefined source hosts and listings from `domain/constants.py`:

- `blogs`
  - Anthropic news
  - Anthropic engineering
  - Medium publications
  - OpenAI index
  - Google AI blog
- `github`
  - GitHub topic pages for AI, LLMs, agents, and machine learning
- `job_postings`
  - LinkedIn only
- `news`
  - WSJ AI
  - WIRED AI tag
  - Times of India technology
  - The Verge AI
- `research_papers`
  - arXiv recent listings for `cs.AI`, `cs.LG`, `cs.CL`, and `cs.IR`

## Current Crawler Behavior

- `MediumCrawler`
  - uses RSS-backed content first when possible
  - this avoids many Cloudflare challenge pages
  - if Medium still blocks a page, the ETL returns a `bot_protection_challenge` error
- `GitHubCrawler`
  - clones the repository
  - extracts content from `README` only
  - skips repositories without a readable README
- `LinkedInJobCrawler`
  - crawls LinkedIn guest job pages only
- `CustomArticleCrawler`
  - handles general web pages
  - uses `AsyncHtmlLoader` and `Html2TextTransformer` when available
  - falls back to `requests` + `BeautifulSoup`

The ETL also rejects shallow pages with too little extracted content.

## Duplicate Handling

The unique source key is `link`.

- if the same `link` already exists in the same target collection, it is skipped as a duplicate
- if the same `link` exists in a different collection, it is moved to the new collection

## Date Filtering

The CLI supports:

- `--start-date`
- `--end-date`

Default behavior:

- `start_date` = 30 days before today
- `end_date` = today

If a document has no `published_at`, it is kept.

## Default Discovery Target

The current default settings in `domain/config.py` are:

- `DISCOVERY_MAX_LINKS=25`
- `DISCOVERY_MIN_PER_CATEGORY=5`

So auto mode targets at least:

- 5 `blogs`
- 5 `github`
- 5 `job_postings`
- 5 `news`
- 5 `research_papers`

This is a target, not a hard network guarantee. Real results still depend on source availability, bot protection, duplicates, and date filtering.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Minimum `.env`:

```bash
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=market_intelligence
```

Recommended:

```bash
USER_AGENT=market-intelligence-etl/1.0
```

Optional:

```bash
GITHUB_API_TOKEN=
```

`GITHUB_API_TOKEN` is optional for public repositories. The current crawler can work without it.

## Run

Auto-discovery:

```bash
python main.py --user "Rafi Atha" --topic "ai engineer"
```

With debug logs:

```bash
python main.py --user "Rafi Atha" --topic "ai engineer" --debug
```

With explicit date range:

```bash
python main.py \
  --user "Rafi Atha" \
  --topic "ai engineer" \
  --start-date 2026-02-19 \
  --end-date 2026-03-19 \
  --debug
```

Manual links:

```bash
python main.py \
  --user "Rafi Atha" \
  --topic "ai engineer" \
  --link "https://github.com/microsoft/autogen" \
  --link "https://arxiv.org/abs/2408.00000" \
  --debug
```

Links from file:

```bash
python main.py --user "Rafi Atha" --topic "ai engineer" --links-file links.txt --debug
```

Help:

```bash
python main.py --help
```

## Debug Output

When `--debug` is enabled, the pipeline prints:

- parsed CLI arguments
- MongoDB setup
- user lookup / creation
- discovered links
- crawler selection for each link
- date-range filtering decisions
- insert / move / duplicate / error results
- final ETL summary

At the end, the CLI prints a JSON run summary.

## Docker

Build:

```bash
docker build -t market-intelligence-etl .
```

Run against MongoDB on your Mac:

```bash
docker run --rm \
  --env-file .env \
  -e MONGODB_URI=mongodb://host.docker.internal:27017 \
  market-intelligence-etl \
  python main.py --user "Rafi Atha" --topic "ai engineer"
```

## Notes

- Medium can still block direct page fetches; predefined auto-discovery is the safest path there.
- LinkedIn guest jobs can change their HTML at any time, so `job_postings` depends on LinkedIn availability.
- GitHub discovery now filters out non-repository pages and ingests README content only.

## Future Improvements

- [ ] Make the `5 per category` target a true refill workflow instead of best-effort discovery
- [ ] Add fallback and retry logic when one category returns too few valid sources
- [ ] Harden crawling against source instability, especially Medium and LinkedIn
- [ ] Improve category classification to reduce misrouted sources
- [ ] Add automated tests for discovery, crawling, duplicate handling, and date filtering
- [ ] Add stronger content-quality validation beyond minimum extracted length
- [ ] Add clearer per-category reporting in the ETL run summary
- [ ] Add scheduling for recurring ETL runs
- [ ] Improve handling for throttling, layout changes, and transient network failures
- [ ] Add a second-stage enrichment layer for smarter tagging or classification later
