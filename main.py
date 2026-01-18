"""LiveFetch - keyless web retrieval with scoring."""

import asyncio, hashlib, re, sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import feedparser, httpx, tldextract, trafilatura
from ddgs import DDGS
from loguru import logger
from newspaper import Article
from rapidfuzz import fuzz
from readability import Document

Mode = Literal["web", "news", "rss"]

@dataclass
class RetrievalIntent:
    query: str
    mode: Mode = "news"
    max_results: int = 8
    freshness_days: Optional[int] = 14
    need_quotes: bool = True
    allow_domains: Optional[List[str]] = None
    deny_domains: Optional[List[str]] = None

@dataclass
class EvidencePacket:
    url: str
    domain: str
    title: Optional[str]
    text: str
    published_at: Optional[datetime]
    fetched_at: datetime
    mode: str
    score: float = 0.0
    score_breakdown: Dict[str, float] = field(default_factory=dict)
    content_sha256: Optional[str] = None
    quotes: List[str] = field(default_factory=list)


class LiveFetch:
    MAX_RESULTS, MAX_CONCURRENCY, TIMEOUT_S = 8, 6, 15.0
    USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) LiveFetchBot/0.1"
    MAX_CHARS, FRESHNESS_HALF_LIFE = 12_000, 7.0
    W_RELEVANCE, W_FRESHNESS, W_TRUST, W_DUP = 0.55, 0.25, 0.20, 0.35

    _SCHEMA = """CREATE TABLE IF NOT EXISTS docs (
        url TEXT PRIMARY KEY, content_sha256 TEXT, fetched_at TEXT,
        published_at TEXT, title TEXT, text TEXT);"""
    _TOKEN_RE = re.compile(r"[a-zA-Z0-9]+")

    def __init__(self, cache_path: str = "livefetch.sqlite"):
        self._conn = sqlite3.connect(cache_path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute(self._SCHEMA)
        self._conn.commit()

    def __enter__(self): return self
    def __exit__(self, *_): self.close()
    def close(self):
        if self._conn: self._conn.close(); self._conn = None

    # discovery
    def discover(self, intent: RetrievalIntent) -> List[str]:
        urls = []
        if intent.mode in ("web", "news"):
            try:
                with DDGS() as ddgs:
                    if intent.mode == "news":
                        for r in ddgs.news(intent.query, max_results=intent.max_results * 2):
                            if u := r.get("url") or r.get("href"): urls.append(u)
                    else:
                        for r in ddgs.text(intent.query, max_results=intent.max_results * 2):
                            if u := r.get("href"): urls.append(u)
            except Exception as e:
                logger.warning(f"DDG discovery failed: {e}")
        elif intent.mode == "rss":
            try:
                for e in feedparser.parse(intent.query).entries[:intent.max_results * 2]:
                    if "link" in e: urls.append(e["link"])
            except Exception as e:
                logger.warning(f"RSS parse failed: {e}")
        seen, out = set(), []
        for u in urls:
            if u not in seen: seen.add(u); out.append(u)
        return out

    # fetch
    async def _fetch_one(self, url: str) -> Dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.TIMEOUT_S, headers={"User-Agent": self.USER_AGENT}, follow_redirects=True) as c:
                r = await c.get(url)
                return {"url": url, "status": r.status_code, "html": r.text if r.status_code == 200 else None,
                        "fetched_at": datetime.now(timezone.utc), "error": None if r.status_code == 200 else f"HTTP {r.status_code}"}
        except Exception as e:
            return {"url": url, "status": 0, "html": None, "fetched_at": datetime.now(timezone.utc), "error": str(e)}

    async def fetch_many(self, urls: List[str]) -> List[Dict[str, Any]]:
        sem = asyncio.Semaphore(self.MAX_CONCURRENCY)
        async def _w(u):
            async with sem: return await self._fetch_one(u)
        return await asyncio.gather(*[_w(u) for u in urls])

    # extraction
    def extract(self, url: str, html: str) -> Tuple[Optional[str], Optional[datetime], Optional[str]]:
        text = trafilatura.extract(html, include_comments=False, include_tables=False, fast=True)
        if text and len(text) > 200:
            _, pub, title = self._newspaper_meta(url, html)
            return text, pub, title
        n_text, pub, title = self._newspaper_meta(url, html)
        if n_text and len(n_text) > 200: return n_text, pub, title
        try:
            doc = Document(html)
            if (summary := doc.summary()) and (r_text := trafilatura.extract(summary)) and len(r_text) > 200:
                return r_text, None, doc.title()
        except: pass
        return None, None, None

    def _newspaper_meta(self, url: str, html: str) -> Tuple[Optional[str], Optional[datetime], Optional[str]]:
        try:
            a = Article(url); a.download(input_html=html); a.parse()
            return a.text or None, a.publish_date, a.title or None
        except: return None, None, None

    # scoring
    def _tokens(self, s: str) -> set: return {t.lower() for t in self._TOKEN_RE.findall(s) if len(t) >= 3}
    def _domain_of(self, url: str) -> str:
        ext = tldextract.extract(url)
        return f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain

    def _relevance(self, query: str, title: str, snippet: str) -> float:
        qtok, text = self._tokens(query), f"{title}\n{snippet}"
        if not qtok: return 0.0
        overlap = len(qtok & self._tokens(text)) / max(1, len(qtok))
        return 0.6 * overlap + 0.4 * fuzz.partial_ratio(query.lower(), text.lower()) / 100.0

    def _freshness(self, published_at: Optional[datetime], now: datetime) -> float:
        if not published_at: return 0.35
        pub = published_at if published_at.tzinfo else published_at.replace(tzinfo=timezone.utc)
        return 2.0 ** (-max(0.0, (now - pub).total_seconds() / 86400.0) / self.FRESHNESS_HALF_LIFE)

    def _trust(self, domain: str) -> float:
        d = domain.lower()
        if d.endswith((".gov", ".gov.uk", ".eu")): return 0.9
        if d.endswith((".edu", ".ac.uk")): return 0.85
        if any(x in d for x in ("reuters", "apnews", "bbc", "nature", "science", "arxiv", "who.int", "un.org")): return 0.8
        if any(x in d for x in ("medium.com", "substack.com")): return 0.55
        return 0.45

    def _redundancy(self, text: str, others: List[str]) -> float:
        return max((fuzz.partial_ratio(text[:2000].lower(), o[:2000].lower()) / 100.0 for o in others), default=0.0)

    def _score(self, query: str, domain: str, title: str, text: str,
               published_at: Optional[datetime], now: datetime, selected: List[str]) -> Tuple[float, Dict[str, float]]:
        R, F, T, D = self._relevance(query, title or "", text[:2000]), self._freshness(published_at, now), self._trust(domain), self._redundancy(text, selected)
        return self.W_RELEVANCE * R + self.W_FRESHNESS * F + self.W_TRUST * T - self.W_DUP * D, {"relevance": R, "freshness": F, "trust": T, "dup": D}

    # quotes
    def _extract_quotes(self, text: str, max_q: int = 5, max_len: int = 240) -> List[str]:
        sents = [x.strip() for x in text.replace("\n", " ").split(".") if len(x.strip()) > 60]
        out = []
        for s in sents[:max_q * 3]:
            if (q := s[:max_len].strip()) and q not in out: out.append(q)
            if len(out) >= max_q: break
        return out

    # cache
    def _cache_get(self, url: str) -> Optional[Dict]:
        row = self._conn.execute("SELECT url, content_sha256, fetched_at, published_at, title, text FROM docs WHERE url=?", (url,)).fetchone()
        return {"url": row[0], "content_sha256": row[1], "fetched_at": row[2], "published_at": row[3], "title": row[4], "text": row[5]} if row else None

    def _cache_put(self, url: str, sha: str, fetched_at: datetime, published_at: Optional[datetime], title: Optional[str], text: str):
        self._conn.execute("INSERT OR REPLACE INTO docs(url, content_sha256, fetched_at, published_at, title, text) VALUES(?,?,?,?,?,?)",
                           (url, sha, fetched_at.isoformat(), published_at.isoformat() if published_at else None, title, text))
        self._conn.commit()

    # pipeline
    async def run(self, intent: RetrievalIntent) -> List[EvidencePacket]:
        logger.info(f"LiveFetch: query='{intent.query}' mode={intent.mode}")
        urls = self.discover(intent)
        logger.debug(f"Discovered {len(urls)} URLs")

        def allowed(u):
            d = self._domain_of(u)
            if intent.allow_domains and d not in intent.allow_domains: return False
            if intent.deny_domains and d in intent.deny_domains: return False
            return True
        urls = [u for u in urls if allowed(u)]

        fetched = await self.fetch_many(urls)
        now = datetime.now(timezone.utc)
        packets, selected_texts = [], []

        for fr in fetched:
            if not fr.get("html"): continue
            text, published_at, title = self.extract(fr["url"], fr["html"])
            if not text: continue

            text = text if len(text) <= self.MAX_CHARS else text[:self.MAX_CHARS] + "\n[...clipped...]"
            sha = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

            if intent.freshness_days and published_at:
                pub = published_at if published_at.tzinfo else published_at.replace(tzinfo=timezone.utc)
                if pub < now - timedelta(days=intent.freshness_days): continue

            domain = self._domain_of(fr["url"])
            score, breakdown = self._score(intent.query, domain, title or "", text, published_at, now, selected_texts)
            packets.append(EvidencePacket(
                url=fr["url"], domain=domain, title=title, text=text, published_at=published_at,
                fetched_at=fr["fetched_at"], mode=intent.mode, score=score, score_breakdown=breakdown,
                content_sha256=sha, quotes=self._extract_quotes(text) if intent.need_quotes else []))

        packets.sort(key=lambda p: p.score, reverse=True)
        final = []
        for p in packets:
            final.append(p); selected_texts.append(p.text)
            if len(final) >= intent.max_results: break

        for p in final:
            self._cache_put(p.url, p.content_sha256 or "", p.fetched_at, p.published_at, p.title, p.text)
        logger.info(f"LiveFetch: returning {len(final)} packets")
        return final

    def run_sync(self, intent: RetrievalIntent) -> List[EvidencePacket]:
        return asyncio.run(self.run(intent))


if __name__ == "__main__":
    import json
    from dataclasses import asdict
    intent = RetrievalIntent(query="latest EU AI Act enforcement timeline", mode="news", max_results=5, freshness_days=30, need_quotes=True)
    with LiveFetch() as lf:
        print(json.dumps([asdict(p) for p in lf.run_sync(intent)], default=str, indent=2))
