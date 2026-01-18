# LiveFetch

Keyless evidence retrieval framework for grounded reasoning systems.

## Overview

LiveFetch provides deterministic, timestamped web evidence for LLM pipelines. It combines discovery, extraction, and multi-factor ranking into a single retrieval primitive that returns structured `EvidencePacket` objects.

## Architecture

```
RetrievalIntent -> [Discovery] -> [Fetch] -> [Extract] -> [Score] -> EvidencePacket[]
                       |             |           |            |
                     DDG/RSS      httpx     trafilatura    composite
                                  async     +fallbacks      ranking
```

## Scoring Model

Documents are ranked by a weighted linear combination:

```
S(d|q) = w_r * R + w_f * F + w_t * T - w_dup * D
```

| Component | Weight | Description |
|-----------|--------|-------------|
| R (relevance) | 0.55 | Token overlap + fuzzy string similarity |
| F (freshness) | 0.25 | Exponential decay: `2^(-age_days / 7)` |
| T (trust) | 0.20 | Domain-based prior (.gov=0.9, .edu=0.85, etc.) |
| D (redundancy) | 0.35 | Similarity penalty to already-selected docs |

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
from tools.DCC.main import LiveFetch, RetrievalIntent

intent = RetrievalIntent(
    query="EU AI Act enforcement",
    mode="news",          # web | news | rss
    max_results=5,
    freshness_days=14,
    need_quotes=True
)

with LiveFetch() as lf:
    packets = lf.run_sync(intent)
```

## Data Structures

### RetrievalIntent

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| query | str | required | Search query or RSS URL |
| mode | Literal | "news" | Discovery mode: web, news, rss |
| max_results | int | 8 | Max packets returned |
| freshness_days | int | 14 | Hard filter cutoff (None=disabled) |
| need_quotes | bool | True | Extract quotable sentences |
| allow_domains | List[str] | None | Whitelist filter |
| deny_domains | List[str] | None | Blacklist filter |

### EvidencePacket

| Field | Type | Description |
|-------|------|-------------|
| url | str | Canonical source URL |
| domain | str | Extracted domain |
| title | str | Page title |
| text | str | Extracted content (clipped to 12k chars) |
| published_at | datetime | Publication timestamp |
| fetched_at | datetime | Retrieval timestamp (UTC) |
| score | float | Composite ranking score |
| score_breakdown | Dict | Individual R/F/T/D components |
| content_sha256 | str | Content hash for deduplication |
| quotes | List[str] | Key sentences for citation |

## Extraction Pipeline

1. **trafilatura** (primary) - fast mode extraction
2. **newspaper3k** (fallback) - also provides publish date inference
3. **readability-lxml** (tertiary) - boilerplate removal + trafilatura cleanup

## Cache

SQLite with WAL mode. Schema:

```sql
CREATE TABLE docs (
    url TEXT PRIMARY KEY,
    content_sha256 TEXT,
    fetched_at TEXT,
    published_at TEXT,
    title TEXT,
    text TEXT
);
```

Default path: `livefetch.sqlite`

## Configuration

Class attributes on `LiveFetch`:

| Attribute | Default | Description |
|-----------|---------|-------------|
| MAX_CONCURRENCY | 6 | Parallel fetch limit |
| TIMEOUT_S | 15.0 | HTTP timeout |
| MAX_CHARS | 12000 | Content truncation |
| FRESHNESS_HALF_LIFE | 7.0 | Days for 50% freshness decay |

## Dependencies

- ddgs (DuckDuckGo search)
- httpx (async HTTP)
- trafilatura, newspaper3k, readability-lxml (extraction)
- rapidfuzz (similarity)
- tldextract (domain parsing)
- feedparser (RSS)
