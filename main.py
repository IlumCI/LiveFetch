import asyncio, hashlib, random, re, sqlite3, time, xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Callable, Dict, List, Literal, Optional, Tuple, Set
from urllib.parse import quote, urlencode

import feedparser, httpx, tldextract, trafilatura
from ddgs import DDGS
from loguru import logger
from newspaper import Article
from rapidfuzz import fuzz
from readability import Document

try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

try:
    from langdetect import detect, DetectorFactory
    DetectorFactory.seed = 0
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False

try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False

try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

Mode = Literal["web", "news", "rss"]


@dataclass
class RIntent:
    query: str
    mode: Mode = "news"
    max_results: int = 8
    freshness_days: Optional[int] = 14
    need_quotes: bool = True
    allow_domains: Optional[List[str]] = None
    deny_domains: Optional[List[str]] = None
    profile: Optional[str] = None
    auto_detect_profile: bool = True
    progress_callback: Optional[Callable[[str, int, int], None]] = None
    content_types: Optional[List[str]] = None
    languages: Optional[List[str]] = None
    need_summary: bool = False

    def __post_init__(self):
        if not self.query or not self.query.strip(): raise ValueError("query cannot be empty")
        if self.max_results < 1: raise ValueError("max_results must be at least 1")
        if self.freshness_days is not None and self.freshness_days < 0: raise ValueError("freshness_days must be non-negative")


@dataclass
class EPacket:
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
    metadata: Dict[str, Any] = field(default_factory=dict)
    images: List[Dict[str, str]] = field(default_factory=list)
    tables: List[Dict[str, Any]] = field(default_factory=list)
    language: Optional[str] = None
    content_type: Optional[str] = None
    sentiment: Optional[float] = None
    summary: Optional[str] = None


@dataclass
class LFConfig:
    cache_path: str = "livefetch.sqlite"
    max_results: int = 8
    max_concurrency: int = 6
    timeout_seconds: float = 15.0
    max_chars: int = 12_000
    freshness_half_life: float = 7.0
    min_text_length: int = 200
    min_score: float = 0.0
    weights: Dict[str, float] = field(default_factory=lambda: {"relevance": 0.55, "freshness": 0.25, "trust": 0.20, "redundancy": 0.35})
    user_agent: str = "Mozilla/5.0 (X11; Linux x86_64) LiveFetchBot/0.1"
    max_retries: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 60.0
    jitter: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 300.0
    rate_limit_per_domain: Dict[str, float] = field(default_factory=dict)
    default_rate_limit: float = 2.0
    user_agents: List[str] = field(default_factory=list)
    proxy: Optional[str] = None
    proxy_rotation: List[str] = field(default_factory=list)
    cache_ttl_days: Optional[int] = None
    max_file_size: Optional[int] = None
    extractors: List[Callable] = field(default_factory=list)


@dataclass
class Profile:
    name: str
    keywords: List[str]
    query_modifiers: List[str]
    allow_domains: List[str]
    deny_domains: List[str] = field(default_factory=list)
    scoring_weights: Dict[str, float] = field(default_factory=dict)
    trust_boost: float = 1.0


class LFError(Exception): pass
class DiscError(LFError): pass
class FetchErr(LFError): pass
class ExtError(LFError): pass


class LF:
    _SCHEMA = "CREATE TABLE IF NOT EXISTS docs (url TEXT PRIMARY KEY, content_sha256 TEXT, fetched_at TEXT, published_at TEXT, title TEXT, text TEXT, expires_at TEXT)"
    _TOKEN_RE = re.compile(r"[a-zA-Z0-9]+")
    _PROFILES: Dict[str, Profile] = {
        "government": Profile(
            name="government",
            keywords=["gdp", "unemployment", "war", "official", "government", "federal", "census", "bureau"],
            query_modifiers=["site:.gov", "site:.gov.uk", "site:.eu", "filetype:pdf"],
            allow_domains=[".gov", ".gov.uk", ".eu", "census.gov", "bls.gov", "treasury.gov"],
            scoring_weights={"relevance": 0.55, "freshness": 0.25, "trust": 0.30, "redundancy": 0.35},
            trust_boost=1.2
        ),
        "market": Profile(
            name="market",
            keywords=["crypto", "bitcoin", "ethereum", "stock", "etf", "price", "trading", "market", "dollar", "currency"],
            query_modifiers=["price", "trading", "market news"],
            allow_domains=["coingecko.com", "coinmarketcap.com", "yahoo.com", "marketwatch.com", "bloomberg.com", "finance.yahoo.com"],
            scoring_weights={"relevance": 0.55, "freshness": 0.30, "trust": 0.20, "redundancy": 0.35},
            trust_boost=1.1
        ),
        "cooking": Profile(
            name="cooking",
            keywords=["recipe", "cooking", "food", "cuisine", "ingredient", "baking", "chef"],
            query_modifiers=["recipe", "how to cook"],
            allow_domains=["allrecipes.com", "foodnetwork.com", "seriouseats.com", "bonappetit.com"]
        ),
        "healthcare": Profile(
            name="healthcare",
            keywords=["health", "medical", "disease", "treatment", "medicine", "symptom", "diagnosis", "patient"],
            query_modifiers=[],
            allow_domains=["who.int", "cdc.gov", "nih.gov", "mayoclinic.org", "webmd.com"],
            scoring_weights={"relevance": 0.55, "freshness": 0.25, "trust": 0.25, "redundancy": 0.35},
            trust_boost=1.15
        ),
        "science": Profile(
            name="science",
            keywords=["research", "study", "scientific", "paper", "journal", "arxiv", "pubmed", "experiment"],
            query_modifiers=["site:.edu", "site:.ac.uk", "filetype:pdf"],
            allow_domains=[".edu", ".ac.uk", "arxiv.org", "pubmed.ncbi.nlm.nih.gov", "nature.com", "science.org"],
            scoring_weights={"relevance": 0.55, "freshness": 0.25, "trust": 0.28, "redundancy": 0.35},
            trust_boost=1.2
        ),
        "academic": Profile(
            name="academic",
            keywords=["academic", "scholar", "research", "paper", "journal", "thesis", "dissertation", "citation"],
            query_modifiers=["site:.edu", "site:.ac.uk", "filetype:pdf", "site:scholar.google.com"],
            allow_domains=[".edu", ".ac.uk", "arxiv.org", "pubmed.ncbi.nlm.nih.gov", "scholar.google.com", "researchgate.net", "academia.edu"],
            scoring_weights={"relevance": 0.55, "freshness": 0.20, "trust": 0.30, "redundancy": 0.35},
            trust_boost=1.25
        ),
        "technical": Profile(
            name="technical",
            keywords=["github", "stackoverflow", "documentation", "api", "code", "programming", "software", "developer"],
            query_modifiers=["site:github.com", "site:stackoverflow.com", "site:docs."],
            allow_domains=["github.com", "stackoverflow.com", "stackexchange.com", "dev.to", "medium.com"],
            scoring_weights={"relevance": 0.60, "freshness": 0.25, "trust": 0.20, "redundancy": 0.35},
            trust_boost=1.1
        ),
        "social": Profile(
            name="social",
            keywords=["reddit", "hacker news", "forum", "discussion", "community", "social"],
            query_modifiers=["site:reddit.com", "site:news.ycombinator.com"],
            allow_domains=["reddit.com", "news.ycombinator.com"],
            scoring_weights={"relevance": 0.55, "freshness": 0.30, "trust": 0.15, "redundancy": 0.35},
            trust_boost=0.9
        ),
        "legal": Profile(
            name="legal",
            keywords=["court", "legal", "law", "case", "judgment", "statute", "regulation", "legislation"],
            query_modifiers=["site:.gov", "filetype:pdf", "site:courts.gov"],
            allow_domains=[".gov", "supremecourt.gov", "uscourts.gov", "law.cornell.edu"],
            scoring_weights={"relevance": 0.55, "freshness": 0.20, "trust": 0.30, "redundancy": 0.35},
            trust_boost=1.3
        )
    }

    def __init__(self, config: Optional[LFConfig] = None, auto_close: bool = False):
        self.config = config or LFConfig()
        self._conn: Optional[sqlite3.Connection] = None
        self._auto_close = auto_close
        self._failure_counts: Dict[str, int] = defaultdict(int)
        self._circuit_breakers: Dict[str, datetime] = {}
        self._rate_limiters: Dict[str, List[float]] = defaultdict(list)
        self._proxy_index = 0
        self._ua_index = 0
        self._metrics: Dict[str, Any] = {"requests": 0, "success": 0, "failures": 0, "cache_hits": 0, "cache_misses": 0, "latencies": []}
        self._nlp_model = None
        if SPACY_AVAILABLE:
            try: self._nlp_model = spacy.load("en_core_web_sm")
            except OSError: logger.warning("spaCy model 'en_core_web_sm' not found. NER will be disabled.")
        self._init_cache()

    def _init_cache(self) -> None:
        try:
            self._conn = sqlite3.connect(self.config.cache_path)
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute(self._SCHEMA)
            try:
                self._conn.execute("ALTER TABLE docs ADD COLUMN expires_at TEXT")
            except sqlite3.OperationalError:
                pass
            self._conn.commit()
            self._cache_cleanup_expired()
        except sqlite3.Error as e:
            logger.error(f"Cache init failed: {e}")
            raise LFError(f"Cache init failed: {e}") from e

    def __enter__(self) -> "LF": return self
    def __exit__(self, *_) -> None: self.close()

    def close(self) -> None:
        if self._conn:
            try: self._conn.close()
            except Exception as e: logger.warning(f"Close error: {e}")
            finally: self._conn = None

    async def discover(self, intent: RIntent) -> List[str]:
        try:
            profiles = self._detect_profile(intent)
            urls = []
            
            if intent.mode == "rss":
                modified_query = self._apply_profile_query(intent)
                modified_intent = RIntent(query=modified_query, mode=intent.mode, max_results=intent.max_results, freshness_days=intent.freshness_days, need_quotes=intent.need_quotes, allow_domains=intent.allow_domains, deny_domains=intent.deny_domains, profile=intent.profile, auto_detect_profile=False)
                urls = self._disc_rss(modified_intent)
            elif "market" in profiles:
                urls = await self._disc_market(intent)
            else:
                query_lower = intent.query.lower()
                if query_lower.startswith("reddit:") or "reddit.com" in query_lower:
                    urls.extend(self._disc_reddit(intent))
                if query_lower.startswith("hn:") or "hacker news" in query_lower or "hackernews" in query_lower:
                    urls.extend(self._disc_hn(intent))
                if query_lower.startswith("arxiv:") or "arxiv" in query_lower or "science" in profiles:
                    urls.extend(self._disc_arxiv(intent))
                if query_lower.startswith("pubmed:") or "pubmed" in query_lower or "healthcare" in profiles or "science" in profiles:
                    urls.extend(self._disc_pubmed(intent))
                if query_lower.startswith("wiki:") or "wikipedia" in query_lower:
                    urls.extend(self._disc_wikipedia(intent))
                if query_lower.startswith("github:") or "github.com" in query_lower or "technical" in profiles:
                    urls.extend(self._disc_github(intent))
                
                if not urls:
                    modified_query = self._apply_profile_query(intent)
                    modified_intent = RIntent(query=modified_query, mode=intent.mode, max_results=intent.max_results, freshness_days=intent.freshness_days, need_quotes=intent.need_quotes, allow_domains=intent.allow_domains, deny_domains=intent.deny_domains, profile=intent.profile, auto_detect_profile=False)
                    urls = self._disc_search(modified_intent)
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            raise DiscError(f"Discovery failed: {e}") from e
        seen, unique = set(), []
        for u in urls:
            if u not in seen: seen.add(u); unique.append(u)
        logger.debug(f"Discovered {len(unique)} unique URLs")
        return unique

    def _disc_search(self, intent: RIntent) -> List[str]:
        urls = []
        try:
            with DDGS() as ddgs:
                max_disc = intent.max_results * 2
                results = ddgs.news(intent.query, max_results=max_disc) if intent.mode == "news" else ddgs.text(intent.query, max_results=max_disc)
                for r in results:
                    if u := r.get("url") or r.get("href"): urls.append(u)
        except Exception as e:
            logger.warning(f"DDG discovery failed: {e}")
            raise
        return urls

    def _disc_rss(self, intent: RIntent) -> List[str]:
        urls = []
        try:
            feed = feedparser.parse(intent.query)
            for e in feed.entries[:intent.max_results * 2]:
                if "link" in e: urls.append(e["link"])
        except Exception as e:
            logger.warning(f"RSS parse failed: {e}")
            raise
        return urls

    async def _disc_market(self, intent: RIntent) -> List[str]:
        urls = []
        query_lower = intent.query.lower()
        query_encoded = quote(intent.query)
        
        crypto_keywords = ["crypto", "bitcoin", "ethereum", "btc", "eth", "token", "coin", "defi", "dex"]
        stock_keywords = ["stock", "etf", "share", "nasdaq", "nyse", "sp500", "dow", "equity"]
        is_crypto = any(kw in query_lower for kw in crypto_keywords)
        is_stock = any(kw in query_lower for kw in stock_keywords)
        
        async with httpx.AsyncClient(timeout=self.config.timeout_seconds, headers={"User-Agent": self._get_user_agent()}) as client:
                if is_crypto:
                    try:
                        search_url = f"https://api.dexscreener.com/latest/dex/search?q={query_encoded}"
                        r = await client.get(search_url)
                        if r.status_code == 200:
                            data = r.json()
                            for pair in data.get("pairs", [])[:intent.max_results * 2]:
                                if pair_url := pair.get("url"):
                                    urls.append(pair_url)
                                elif pair_address := pair.get("pairAddress"):
                                    chain = pair.get("chainId", "ethereum")
                                    urls.append(f"https://dexscreener.com/{chain}/{pair_address}")
                    except Exception as e:
                        logger.debug(f"Dexscreener search failed: {e}")
                    
                    try:
                        boosts_url = "https://api.dexscreener.com/token-boosts/top/v1"
                        r = await client.get(boosts_url)
                        if r.status_code == 200:
                            boosts = r.json()
                            if isinstance(boosts, list):
                                for boost in boosts[:intent.max_results]:
                                    if isinstance(boost, dict):
                                        chain_id = boost.get("chainId", "ethereum")
                                        pair_address = boost.get("pairAddress")
                                        if pair_address:
                                            urls.append(f"https://dexscreener.com/{chain_id}/{pair_address}")
                    except Exception as e:
                        logger.debug(f"Dexscreener boosts failed: {e}")
                    
                    try:
                        coingecko_url = f"https://api.coingecko.com/api/v3/search?query={query_encoded}"
                        r = await client.get(coingecko_url)
                        if r.status_code == 200:
                            data = r.json()
                            for coin in data.get("coins", [])[:intent.max_results]:
                                if coin_id := coin.get("id"):
                                    urls.append(f"https://www.coingecko.com/en/coins/{coin_id}")
                    except Exception as e:
                        logger.debug(f"CoinGecko API failed: {e}")
                    
                    try:
                        trending_url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false"
                        r = await client.get(trending_url)
                        if r.status_code == 200:
                            data = r.json()
                            for coin in data[:intent.max_results]:
                                if coin_id := coin.get("id"):
                                    urls.append(f"https://www.coingecko.com/en/coins/{coin_id}")
                    except Exception as e:
                        logger.debug(f"CoinGecko trending failed: {e}")
                
                if is_stock:
                    try:
                        yahoo_search = f"https://query1.finance.yahoo.com/v1/finance/search?q={query_encoded}&quotesCount=10&newsCount=5"
                        r = await client.get(yahoo_search)
                        if r.status_code == 200:
                            data = r.json()
                            for quote_item in data.get("quotes", [])[:intent.max_results]:
                                if symbol := quote_item.get("symbol"):
                                    urls.append(f"https://finance.yahoo.com/quote/{symbol}")
                            for news in data.get("news", [])[:intent.max_results]:
                                if news_url := news.get("link"):
                                    urls.append(news_url)
                    except Exception as e:
                        logger.debug(f"Yahoo Finance API failed: {e}")
                
                if not is_crypto and not is_stock:
                    try:
                        yahoo_search = f"https://query1.finance.yahoo.com/v1/finance/search?q={query_encoded}&quotesCount=5&newsCount=10"
                        r = await client.get(yahoo_search)
                        if r.status_code == 200:
                            data = r.json()
                            for quote_item in data.get("quotes", [])[:5]:
                                if symbol := quote_item.get("symbol"):
                                    urls.append(f"https://finance.yahoo.com/quote/{symbol}")
                            for news in data.get("news", [])[:10]:
                                if news_url := news.get("link"):
                                    urls.append(news_url)
                    except Exception as e:
                        logger.debug(f"Yahoo Finance search failed: {e}")
        
        if not urls:
            modified_query = self._apply_profile_query(intent)
            modified_intent = RIntent(query=modified_query, mode=intent.mode, max_results=intent.max_results, freshness_days=intent.freshness_days, need_quotes=intent.need_quotes, allow_domains=intent.allow_domains, deny_domains=intent.deny_domains, profile=intent.profile, auto_detect_profile=False)
            urls = self._disc_search(modified_intent)
        return urls

    def _disc_reddit(self, intent: RIntent) -> List[str]:
        urls = []
        try:
            query_encoded = quote(intent.query)
            subreddit = "all"
            if "r/" in intent.query.lower():
                parts = intent.query.lower().split("r/")
                if len(parts) > 1:
                    subreddit = parts[1].split()[0].split("/")[0]
                    query_encoded = quote(" ".join(parts[1].split()[1:]) if len(parts[1].split()) > 1 else intent.query)
            url = f"https://www.reddit.com/r/{subreddit}/search.json?q={query_encoded}&restrict_sr=1&limit={intent.max_results * 2}"
            async def _fetch():
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds, headers={"User-Agent": self._get_user_agent()}) as c:
                    r = await c.get(url)
                    if r.status_code == 200:
                        data = r.json()
                        for child in data.get("data", {}).get("children", [])[:intent.max_results * 2]:
                            if url_val := child.get("data", {}).get("url"): urls.append(url_val)
            asyncio.run(_fetch())
        except Exception as e: logger.warning(f"Reddit discovery failed: {e}")
        return urls

    def _disc_hn(self, intent: RIntent) -> List[str]:
        urls = []
        try:
            query_encoded = quote(intent.query)
            url = f"https://hn.algolia.com/api/v1/search?query={query_encoded}&hitsPerPage={intent.max_results * 2}"
            async def _fetch():
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as c:
                    r = await c.get(url)
                    if r.status_code == 200:
                        data = r.json()
                        for hit in data.get("hits", [])[:intent.max_results * 2]:
                            if url_val := hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID')}": urls.append(url_val)
            asyncio.run(_fetch())
        except Exception as e: logger.warning(f"HN discovery failed: {e}")
        return urls

    def _disc_arxiv(self, intent: RIntent) -> List[str]:
        urls = []
        try:
            query_encoded = quote(intent.query.replace(" ", "+AND+"))
            url = f"http://export.arxiv.org/api/query?search_query={query_encoded}&max_results={intent.max_results * 2}"
            async def _fetch():
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as c:
                    r = await c.get(url)
                    if r.status_code == 200:
                        root = ET.fromstring(r.text)
                        ns = {"atom": "http://www.w3.org/2005/Atom"}
                        for entry in root.findall("atom:entry", ns)[:intent.max_results * 2]:
                            if id_elem := entry.find("atom:id", ns): urls.append(id_elem.text)
            asyncio.run(_fetch())
        except Exception as e: logger.warning(f"ArXiv discovery failed: {e}")
        return urls

    def _disc_pubmed(self, intent: RIntent) -> List[str]:
        urls = []
        try:
            query_encoded = quote(intent.query)
            search_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={query_encoded}&retmax={intent.max_results * 2}"
            async def _fetch():
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as c:
                    r = await c.get(search_url)
                    if r.status_code == 200:
                        root = ET.fromstring(r.text)
                        pmids = [id_elem.text for id_elem in root.findall(".//Id")[:intent.max_results * 2]]
                        for pmid in pmids: urls.append(f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/")
            asyncio.run(_fetch())
        except Exception as e: logger.warning(f"PubMed discovery failed: {e}")
        return urls

    def _disc_wikipedia(self, intent: RIntent) -> List[str]:
        urls = []
        try:
            query_encoded = quote(intent.query)
            url = f"https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch={query_encoded}&format=json&srlimit={intent.max_results * 2}"
            async def _fetch():
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as c:
                    r = await c.get(url)
                    if r.status_code == 200:
                        data = r.json()
                        for result in data.get("query", {}).get("search", [])[:intent.max_results * 2]:
                            title = result.get("title", "").replace(" ", "_")
                            urls.append(f"https://en.wikipedia.org/wiki/{title}")
            asyncio.run(_fetch())
        except Exception as e: logger.warning(f"Wikipedia discovery failed: {e}")
        return urls

    def _disc_github(self, intent: RIntent) -> List[str]:
        urls = []
        try:
            query_encoded = quote(intent.query)
            url = f"https://api.github.com/search/repositories?q={query_encoded}&per_page={min(intent.max_results * 2, 30)}"
            async def _fetch():
                async with httpx.AsyncClient(timeout=self.config.timeout_seconds, headers={"User-Agent": self._get_user_agent()}) as c:
                    r = await c.get(url)
                    if r.status_code == 200:
                        data = r.json()
                        for repo in data.get("items", [])[:intent.max_results * 2]:
                            if html_url := repo.get("html_url"): urls.append(html_url)
            asyncio.run(_fetch())
        except Exception as e: logger.warning(f"GitHub discovery failed: {e}")
        return urls

    def _get_user_agent(self) -> str:
        if self.config.user_agents:
            ua = self.config.user_agents[self._ua_index % len(self.config.user_agents)]
            self._ua_index += 1; return ua
        return self.config.user_agent

    def _get_proxy(self) -> Optional[str]:
        if self.config.proxy_rotation:
            proxy = self.config.proxy_rotation[self._proxy_index % len(self.config.proxy_rotation)]
            self._proxy_index += 1; return proxy
        return self.config.proxy

    def _check_circuit_breaker(self, domain: str) -> bool:
        if domain in self._circuit_breakers:
            if datetime.now(timezone.utc) < self._circuit_breakers[domain]: return False
            del self._circuit_breakers[domain]
        return True

    def _rate_limit_wait(self, domain: str) -> None:
        rate = self.config.rate_limit_per_domain.get(domain, self.config.default_rate_limit)
        now = time.time()
        self._rate_limiters[domain] = [t for t in self._rate_limiters[domain] if now - t < 1.0]
        if len(self._rate_limiters[domain]) >= rate:
            sleep_time = 1.0 - (now - self._rate_limiters[domain][0])
            if sleep_time > 0: time.sleep(sleep_time)
        self._rate_limiters[domain].append(time.time())

    async def _fetch_one_with_retry(self, url: str) -> Dict[str, Any]:
        domain = self._ext_domain(url)
        if not self._check_circuit_breaker(domain):
            return {"url": url, "status": 0, "html": None, "fetched_at": datetime.now(timezone.utc), "error": "Circuit breaker open"}
        
        self._rate_limit_wait(domain)
        start_time = time.time()
        
        for attempt in range(self.config.max_retries + 1):
            try:
                proxy = self._get_proxy()
                headers = {"User-Agent": self._get_user_agent()}
                client_kwargs = {"timeout": self.config.timeout_seconds, "headers": headers, "follow_redirects": True}
                if proxy:
                    client_kwargs["proxies"] = {"http://": proxy, "https://": proxy}
                
                async with httpx.AsyncClient(**client_kwargs) as c:
                    if self.config.max_file_size:
                        head_resp = await c.head(url)
                        content_length = head_resp.headers.get("Content-Length")
                        if content_length and int(content_length) > self.config.max_file_size: return {"url": url, "status": 0, "html": None, "fetched_at": datetime.now(timezone.utc), "error": f"File too large: {content_length} bytes"}
                    r = await c.get(url)
                    latency = time.time() - start_time
                    self._metrics["requests"] += 1
                    self._metrics["latencies"].append(latency)
                    if len(self._metrics["latencies"]) > 1000:
                        self._metrics["latencies"] = self._metrics["latencies"][-1000:]
                    
                    if r.status_code == 200:
                        self._metrics["success"] += 1; self._failure_counts[domain] = 0
                        return {"url": url, "status": r.status_code, "html": r.text, "fetched_at": datetime.now(timezone.utc), "error": None, "content_type": r.headers.get("Content-Type", "").split(";")[0]}
                    else:
                        self._metrics["failures"] += 1
                        if attempt < self.config.max_retries:
                            backoff = min(self.config.backoff_base ** attempt, self.config.backoff_max)
                            if self.config.jitter: backoff *= (0.5 + random.random() * 0.5)
                            await asyncio.sleep(backoff); continue
                        return {"url": url, "status": r.status_code, "html": None, "fetched_at": datetime.now(timezone.utc), "error": f"HTTP {r.status_code}"}
            except Exception as e:
                self._metrics["failures"] += 1
                self._failure_counts[domain] += 1
                if self._failure_counts[domain] >= self.config.circuit_breaker_threshold:
                    self._circuit_breakers[domain] = datetime.now(timezone.utc) + timedelta(seconds=self.config.circuit_breaker_timeout)
                    logger.warning(f"Circuit breaker opened for {domain}")
                if attempt < self.config.max_retries:
                    backoff = min(self.config.backoff_base ** attempt, self.config.backoff_max)
                    if self.config.jitter: backoff *= (0.5 + random.random() * 0.5)
                    await asyncio.sleep(backoff); continue
                logger.debug(f"Fetch failed for {url}: {e}")
            return {"url": url, "status": 0, "html": None, "fetched_at": datetime.now(timezone.utc), "error": str(e)}
        
        return {"url": url, "status": 0, "html": None, "fetched_at": datetime.now(timezone.utc), "error": "Max retries exceeded"}

    async def _fetch_one(self, url: str) -> Dict[str, Any]:
        return await self._fetch_one_with_retry(url)

    async def fetch_many(self, urls: List[str]) -> List[Dict[str, Any]]:
        sem = asyncio.Semaphore(self.config.max_concurrency)
        async def _w(u):
            async with sem: return await self._fetch_one(u)
        return await asyncio.gather(*[_w(u) for u in urls])

    def _ext_pdf(self, url: str, content: bytes) -> Tuple[Optional[str], Optional[datetime], Optional[str]]:
        if not PDF_AVAILABLE: return None, None, None
        try:
            import io
            pdf = pdfplumber.open(io.BytesIO(content))
            text_parts = []
            for page in pdf.pages[:50]:
                if page_text := page.extract_text(): text_parts.append(page_text)
            pdf.close()
            text = "\n\n".join(text_parts)
            return text[:self.config.max_chars] if text else None, None, None
        except Exception as e: logger.debug(f"PDF extraction failed for {url}: {e}"); return None, None, None

    def _ext_structured_data(self, html: str) -> Dict[str, Any]:
        metadata = {}
        if not BS4_AVAILABLE: return metadata
        try:
            soup = BeautifulSoup(html, "html.parser")
            json_ld = soup.find_all("script", type="application/ld+json")
            for script in json_ld:
                try:
                    import json
                    data = json.loads(script.string)
                    if isinstance(data, dict): metadata.update(data)
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict): metadata.update(item)
                except Exception: pass
            
            og_tags = soup.find_all("meta", property=lambda x: x and x.startswith("og:"))
            for tag in og_tags:
                prop = tag.get("property", "").replace("og:", "")
                content = tag.get("content", "")
                if prop and content: metadata[f"og:{prop}"] = content
            
            microdata = soup.find_all(attrs={"itemscope": True})
            for item in microdata[:5]:
                item_type = item.get("itemtype", "")
                if item_type:
                    metadata["microdata"] = metadata.get("microdata", [])
                    metadata["microdata"].append({"type": item_type})
        except Exception as e:
            logger.debug(f"Structured data extraction failed: {e}")
        return metadata

    def _ext_tables(self, html: str) -> List[Dict[str, Any]]:
        tables = []
        if not PANDAS_AVAILABLE or not BS4_AVAILABLE:
            return tables
        try:
            soup = BeautifulSoup(html, "html.parser")
            html_tables = soup.find_all("table")[:10]
            for table in html_tables:
                try:
                    df = pd.read_html(str(table))[0]
                    tables.append({"columns": df.columns.tolist(), "data": df.values.tolist()[:100]})
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Table extraction failed: {e}")
        return tables

    def _ext_images(self, html: str, base_url: str) -> List[Dict[str, str]]:
        images = []
        if not BS4_AVAILABLE:
            return images
        try:
            soup = BeautifulSoup(html, "html.parser")
            img_tags = soup.find_all("img")[:20]
            for img in img_tags:
                src = img.get("src", "")
                if src:
                    if src.startswith("//"):
                        src = "https:" + src
                    elif src.startswith("/"):
                        from urllib.parse import urljoin
                        src = urljoin(base_url, src)
                    images.append({"url": src, "alt": img.get("alt", ""), "title": img.get("title", "")})
        except Exception as e:
            logger.debug(f"Image extraction failed: {e}")
        return images

    def _detect_language(self, text: str) -> Optional[str]:
        if not LANGDETECT_AVAILABLE or not text:
            return None
        try:
            return detect(text[:1000])
        except Exception:
            return None

    def _analyze_sentiment(self, text: str) -> Optional[float]:
        if not TEXTBLOB_AVAILABLE or not text:
            return None
        try:
            blob = TextBlob(text[:5000])
            return blob.sentiment.polarity
        except Exception:
            return None

    def _extract_entities(self, text: str) -> Dict[str, List[str]]:
        entities = {"PERSON": [], "ORG": [], "GPE": [], "DATE": []}
        if not SPACY_AVAILABLE or not self._nlp_model or not text:
            return entities
        try:
            doc = self._nlp_model(text[:10000])
            for ent in doc.ents:
                if ent.label_ in entities:
                    entities[ent.label_].append(ent.text)
        except Exception:
            pass
        return entities

    def _summarize(self, text: str, max_sentences: int = 3) -> Optional[str]:
        if not text or len(text) < 200:
            return None
        try:
            sentences = [s.strip() for s in text.replace("\n", " ").split(".") if len(s.strip()) > 20]
            if len(sentences) <= max_sentences:
                return text
            words = text.lower().split()
            word_freq = {}
            for word in words:
                if len(word) > 3:
                    word_freq[word] = word_freq.get(word, 0) + 1
            max_freq = max(word_freq.values()) if word_freq else 1
            sentence_scores = {}
            for sentence in sentences:
                score = sum(word_freq.get(word.lower(), 0) / max_freq for word in sentence.split() if len(word) > 3)
                sentence_scores[sentence] = score
            top_sentences = sorted(sentence_scores.items(), key=lambda x: x[1], reverse=True)[:max_sentences]
            return ". ".join([s[0] for s in sorted(top_sentences, key=lambda x: sentences.index(x[0]))])
        except Exception:
            return None

    def _extract_keywords(self, text: str, max_keywords: int = 10) -> List[str]:
        if not text:
            return []
        try:
            words = [w.lower() for w in re.findall(r"\b[a-zA-Z]{4,}\b", text)]
            word_freq = {}
            for word in words:
                word_freq[word] = word_freq.get(word, 0) + 1
            return [w for w, _ in sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:max_keywords]]
        except Exception:
            return []

    def extract(self, url: str, html: str, content_type: Optional[str] = None) -> Tuple[Optional[str], Optional[datetime], Optional[str], Dict[str, Any], List[Dict[str, str]], List[Dict[str, Any]], Optional[str], Optional[str], Optional[float], Optional[str]]:
        metadata = {}
        images = []
        tables = []
        language = None
        detected_content_type = content_type
        sentiment = None
        summary = None
        
        if any(x in url.lower() for x in ["dexscreener.com", "coingecko.com", "coinmarketcap.com", "finance.yahoo.com", "marketwatch.com", "bloomberg.com"]):
            market_data = self._ext_market_data(url, html)
            if market_data:
                metadata["market_data"] = market_data.split("\n\n")[0] if "\n\n" in market_data else market_data
                return market_data, None, None, metadata, images, tables, language, detected_content_type, sentiment, summary
        
        if detected_content_type == "application/pdf" or url.lower().endswith(".pdf"):
            try:
                async def _fetch_pdf():
                    async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as c:
                        r = await c.get(url)
                        if r.status_code == 200:
                            return r.content
                    return None
                pdf_content = asyncio.run(_fetch_pdf())
                if pdf_content:
                    text, _, title = self._ext_pdf(url, pdf_content)
                    if text:
                        language = self._detect_language(text)
                        sentiment = self._analyze_sentiment(text)
                        return text, None, title, metadata, images, tables, language, "application/pdf", sentiment, summary
            except Exception:
                pass
        
        text = trafilatura.extract(html, include_comments=False, include_tables=False, fast=True)
        if text and len(text) > self.config.min_text_length:
            _, pub, title = self._ext_np_meta(url, html)
            metadata.update(self._ext_structured_data(html))
            images = self._ext_images(html, url)
            tables = self._ext_tables(html)
            language = self._detect_language(text)
            sentiment = self._analyze_sentiment(text)
            metadata["keywords"] = self._extract_keywords(text)
            if self._nlp_model:
                metadata["entities"] = self._extract_entities(text)
            return text, pub, title, metadata, images, tables, language, detected_content_type or "text/html", sentiment, summary
        
        n_text, pub, title = self._ext_np_meta(url, html)
        if n_text and len(n_text) > self.config.min_text_length:
            metadata.update(self._ext_structured_data(html))
            images = self._ext_images(html, url)
            language = self._detect_language(n_text)
            sentiment = self._analyze_sentiment(n_text)
            return n_text, pub, title, metadata, images, tables, language, detected_content_type or "text/html", sentiment, summary
        
        try:
            doc = Document(html)
            if (s := doc.summary()) and (r_text := trafilatura.extract(s)) and len(r_text) > self.config.min_text_length:
                metadata.update(self._ext_structured_data(html))
                language = self._detect_language(r_text)
                return r_text, None, doc.title(), metadata, images, tables, language, detected_content_type or "text/html", sentiment, summary
        except Exception: pass
        return None, None, None, metadata, images, tables, language, detected_content_type, sentiment, summary

    def _ext_market_data(self, url: str, html: str) -> Optional[str]:
        try:
            import re
            market_data = {}
            data_parts = []
            
            if "dexscreener.com" in url.lower():
                price_match = re.search(r'"priceUsd"\s*:\s*"?([\d.]+)"?', html, re.IGNORECASE)
                if price_match:
                    price = price_match.group(1)
                    market_data["price"] = price
                    data_parts.append(f"Price: ${price}")
                vol_match = re.search(r'"volume"\s*:\s*\{[^}]*"h24"\s*:\s*([\d.]+)', html, re.IGNORECASE)
                if vol_match:
                    vol = vol_match.group(1)
                    market_data["volume_24h"] = vol
                    data_parts.append(f"24h Volume: ${vol}")
                fdv_match = re.search(r'"fdv"\s*:\s*([\d.]+)', html, re.IGNORECASE)
                if fdv_match:
                    fdv = fdv_match.group(1)
                    market_data["fdv"] = fdv
                    data_parts.append(f"FDV: ${fdv}")
                change_match = re.search(r'"priceChange"\s*:\s*\{[^}]*"h24"\s*:\s*([+-]?[\d.]+)', html, re.IGNORECASE)
                if change_match:
                    change = change_match.group(1)
                    market_data["change_24h"] = change
                    data_parts.append(f"24h Change: {change}%")
            
            if "coingecko.com" in url.lower():
                price_match = re.search(r'\$?([\d,]+\.?\d*)\s*(?:USD|usd)', html, re.IGNORECASE)
                if price_match:
                    price = price_match.group(1).replace(',', '')
                    market_data["price"] = price
                    data_parts.append(f"Price: ${price}")
                mcap_match = re.search(r'Market\s*Cap[:\s]*\$?([\d,]+\.?\d*)\s*(?:B|M|billion|million)?', html, re.IGNORECASE)
                if mcap_match:
                    mcap = mcap_match.group(1).replace(',', '')
                    market_data["market_cap"] = mcap
                    data_parts.append(f"Market Cap: ${mcap}")
                vol_match = re.search(r'Volume[:\s]*\$?([\d,]+\.?\d*)\s*(?:B|M|billion|million)?', html, re.IGNORECASE)
                if vol_match:
                    vol = vol_match.group(1).replace(',', '')
                    market_data["volume"] = vol
                    data_parts.append(f"Volume: ${vol}")
                change_match = re.search(r'([+-]?[\d,]+\.?\d*%?)', html[:10000], re.IGNORECASE)
                if change_match:
                    change = change_match.group(1)
                    market_data["change_24h"] = change
                    data_parts.append(f"24h Change: {change}")
            
            if "coinmarketcap.com" in url.lower():
                price_match = re.search(r'\$?([\d,]+\.?\d*)\s*(?:USD|usd)', html, re.IGNORECASE)
                if price_match:
                    price = price_match.group(1).replace(',', '')
                    market_data["price"] = price
                    data_parts.append(f"Price: ${price}")
                mcap_match = re.search(r'Market\s*Cap[:\s]*\$?([\d,]+\.?\d*)\s*(?:B|M|billion|million)?', html, re.IGNORECASE)
                if mcap_match:
                    mcap = mcap_match.group(1).replace(',', '')
                    market_data["market_cap"] = mcap
                    data_parts.append(f"Market Cap: ${mcap}")
            
            if "finance.yahoo.com" in url.lower():
                price_match = re.search(r'"regularMarketPrice"\s*:\s*([\d.]+)', html, re.IGNORECASE)
                if not price_match:
                    price_match = re.search(r'[\$]?([\d,]+\.?\d*)\s*(?:USD|usd)?', html[:10000], re.IGNORECASE)
                if price_match:
                    price = price_match.group(1).replace(',', '')
                    market_data["price"] = price
                    data_parts.append(f"Price: ${price}")
                change_match = re.search(r'"regularMarketChangePercent"\s*:\s*([+-]?[\d.]+)', html, re.IGNORECASE)
                if not change_match:
                    change_match = re.search(r'([+-]?[\d,]+\.?\d*%?)', html[:10000])
                if change_match:
                    change = change_match.group(1)
                    market_data["change"] = change
                    data_parts.append(f"Change: {change}%")
                mcap_match = re.search(r'"marketCap"\s*:\s*\{[^}]*"raw"\s*:\s*([\d]+)', html, re.IGNORECASE)
                if mcap_match:
                    mcap = mcap_match.group(1)
                    market_data["market_cap"] = mcap
                    data_parts.append(f"Market Cap: ${mcap}")
            
            if "marketwatch.com" in url.lower() or "bloomberg.com" in url.lower():
                price_match = re.search(r'[\$]?([\d,]+\.?\d*)\s*(?:USD|usd)?', html[:10000], re.IGNORECASE)
                if price_match:
                    price = price_match.group(1).replace(',', '')
                    market_data["price"] = price
                    data_parts.append(f"Price: ${price}")
                change_match = re.search(r'([+-]?[\d,]+\.?\d*%?)', html[:10000])
                if change_match:
                    change = change_match.group(1)
                    market_data["change"] = change
                    data_parts.append(f"Change: {change}")
            
            if data_parts:
                text = trafilatura.extract(html, include_comments=False, include_tables=False, fast=True) or ""
                market_info = " | ".join(data_parts)
                return f"{market_info}\n\n{text[:1000]}" if len(text) > 100 else market_info
        except Exception as e:
            logger.debug(f"Market data extraction failed: {e}")
        return None

    def _ext_np_meta(self, url: str, html: str) -> Tuple[Optional[str], Optional[datetime], Optional[str]]:
        try:
            a = Article(url); a.download(input_html=html); a.parse()
            return a.text or None, a.publish_date, a.title or None
        except Exception: return None, None, None

    def _tokenize(self, text: str) -> Set[str]: return {t.lower() for t in self._TOKEN_RE.findall(text) if len(t) >= 3}

    def _ext_domain(self, url: str) -> str:
        ext = tldextract.extract(url)
        return f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain

    def _score_rel(self, query: str, title: str, snippet: str) -> float:
        qtok = self._tokenize(query)
        if not qtok: return 0.0
        text_tok = self._tokenize(f"{title}\n{snippet}")
        overlap = len(qtok & text_tok) / max(1, len(qtok))
        fuzzy = fuzz.partial_ratio(query.lower(), f"{title}\n{snippet}".lower()) / 100.0
        return 0.6 * overlap + 0.4 * fuzzy

    def _score_fresh(self, pub_at: Optional[datetime], now: datetime) -> float:
        if not pub_at: return 0.35
        pub = pub_at if pub_at.tzinfo else pub_at.replace(tzinfo=timezone.utc)
        return 2.0 ** (-max(0.0, (now - pub).total_seconds() / 86400.0) / self.config.freshness_half_life)

    def _score_trust(self, domain: str) -> float:
        d = domain.lower()
        if d.endswith((".gov", ".gov.uk", ".eu")): return 0.9
        if d.endswith((".edu", ".ac.uk")): return 0.85
        if any(x in d for x in ("reuters", "apnews", "bbc", "nature", "science", "arxiv", "who.int", "un.org")): return 0.8
        if any(x in d for x in ("medium.com", "substack.com")): return 0.55
        return 0.45

    def _score_red(self, text: str, others: List[str]) -> float:
        if not others: return 0.0
        return max((fuzz.partial_ratio(text[:2000].lower(), o[:2000].lower()) / 100.0 for o in others), default=0.0)

    def _calc_score(self, query: str, domain: str, title: str, text: str, pub_at: Optional[datetime], now: datetime, selected: List[str], profiles: Optional[List[str]] = None) -> Tuple[float, Dict[str, float]]:
        w = self.config.weights.copy()
        trust_boost = 1.0
        if profiles:
            for pname in profiles:
                profile = self._PROFILES[pname]
                if profile.scoring_weights:
                    for key, val in profile.scoring_weights.items():
                        w[key] = val
                trust_boost = max(trust_boost, profile.trust_boost)
        R, F, T, D = self._score_rel(query, title or "", text[:2000]), self._score_fresh(pub_at, now), self._score_trust(domain) * trust_boost, self._score_red(text, selected)
        T = min(T, 1.0)
        return w["relevance"] * R + w["freshness"] * F + w["trust"] * T - w["redundancy"] * D, {"relevance": R, "freshness": F, "trust": T, "redundancy": D}

    def _validate(self, text: str, score: float, sha: Optional[str], seen: Set[str]) -> bool:
        if not text or len(text.strip()) < self.config.min_text_length: return False
        if score < self.config.min_score: return False
        if sha and sha in seen: return False
        return True

    def _is_dup(self, sha: Optional[str], seen: Set[str]) -> bool: return sha is not None and sha in seen

    def _ext_quotes(self, text: str, max_q: int = 5, max_len: int = 240) -> List[str]:
        sents = [s.strip() for s in text.replace("\n", " ").split(".") if len(s.strip()) > 60]
        quotes = []
        for s in sents[:max_q * 3]:
            if (q := s[:max_len].strip()) and q not in quotes: quotes.append(q)
            if len(quotes) >= max_q: break
        return quotes

    def _cache_get(self, url: str) -> Optional[Dict[str, Any]]:
        if not self._conn: return None
        try:
            row = self._conn.execute("SELECT url, content_sha256, fetched_at, published_at, title, text, expires_at FROM docs WHERE url=?", (url,)).fetchone()
            if not row: return None
            expires_at_str = row[6]
            if expires_at_str and self.config.cache_ttl_days:
                expires_at = datetime.fromisoformat(expires_at_str)
                if datetime.now(timezone.utc) > expires_at:
                    self._conn.execute("DELETE FROM docs WHERE url=?", (url,))
                    self._conn.commit()
                    self._metrics["cache_misses"] += 1
                    return None
            self._metrics["cache_hits"] += 1
            return {"url": row[0], "content_sha256": row[1], "fetched_at": row[2], "published_at": row[3], "title": row[4], "text": row[5]}
        except sqlite3.Error as e: logger.warning(f"Cache get failed: {e}"); return None

    def _cache_put(self, url: str, sha: str, fetched_at: datetime, pub_at: Optional[datetime], title: Optional[str], text: str) -> None:
        if not self._conn: return
        try:
            expires_at = None
            if self.config.cache_ttl_days:
                expires_at = (fetched_at + timedelta(days=self.config.cache_ttl_days)).isoformat()
            self._conn.execute("INSERT OR REPLACE INTO docs(url, content_sha256, fetched_at, published_at, title, text, expires_at) VALUES(?,?,?,?,?,?,?)", (url, sha, fetched_at.isoformat(), pub_at.isoformat() if pub_at else None, title, text, expires_at))
            self._conn.commit()
        except sqlite3.Error as e: logger.warning(f"Cache put failed: {e}")

    def _cache_cleanup_expired(self) -> None:
        if not self._conn or not self.config.cache_ttl_days: return
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=self.config.cache_ttl_days)).isoformat()
            self._conn.execute("DELETE FROM docs WHERE expires_at < ?", (cutoff,))
            self._conn.commit()
        except sqlite3.Error as e: logger.warning(f"Cache cleanup failed: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        if not self._conn: return {}
        try:
            total = self._conn.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
            expired = 0
            if self.config.cache_ttl_days:
                cutoff = (datetime.now(timezone.utc) - timedelta(days=self.config.cache_ttl_days)).isoformat()
                expired = self._conn.execute("SELECT COUNT(*) FROM docs WHERE expires_at < ?", (cutoff,)).fetchone()[0]
            return {"total": total, "expired": expired, "hits": self._metrics.get("cache_hits", 0), "misses": self._metrics.get("cache_misses", 0)}
        except sqlite3.Error: return {}

    def warm_cache(self, urls: Optional[List[str]] = None) -> None:
        if not self._conn or not self.config.cache_ttl_days: return
        try:
            if urls:
                for url in urls:
                    row = self._conn.execute("SELECT url, expires_at FROM docs WHERE url=?", (url,)).fetchone()
                    if row and row[1]:
                        expires_at = datetime.fromisoformat(row[1])
                        if (expires_at - datetime.now(timezone.utc)).total_seconds() < 86400:
                            asyncio.run(self._fetch_one_with_retry(url))
            else:
                cutoff = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
                rows = self._conn.execute("SELECT url FROM docs WHERE expires_at < ? AND expires_at > ?", (cutoff, datetime.now(timezone.utc).isoformat())).fetchall()
                for row in rows[:10]:
                    asyncio.run(self._fetch_one_with_retry(row[0]))
        except Exception as e:
            logger.warning(f"Cache warming failed: {e}")

    def _extract_wikipedia(self, url: str, html: str, text: str) -> Optional[Dict[str, Any]]:
        if "wikipedia.org" not in url.lower():
            return None
        try:
            if BS4_AVAILABLE:
                soup = BeautifulSoup(html, "html.parser")
                infobox = soup.find("table", class_="infobox")
                if infobox:
                    return {"infobox": str(infobox)}
        except Exception:
            pass
        return None

    def _extract_github(self, url: str, html: str, text: str) -> Optional[Dict[str, Any]]:
        if "github.com" not in url.lower():
            return None
        try:
            metadata = {}
            if "/blob/" in url:
                metadata["type"] = "file"
                parts = url.split("/blob/")
                if len(parts) > 1:
                    metadata["file_path"] = parts[1]
            elif "/tree/" in url:
                metadata["type"] = "directory"
            else:
                metadata["type"] = "repository"
            return metadata
        except Exception:
            pass
        return None

    def _extract_stackoverflow(self, url: str, html: str, text: str) -> Optional[Dict[str, Any]]:
        if "stackoverflow.com" not in url.lower() and "stackexchange.com" not in url.lower():
            return None
        try:
            metadata = {"type": "qna"}
            if BS4_AVAILABLE:
                soup = BeautifulSoup(html, "html.parser")
                votes = soup.find("div", class_="js-vote-count")
                if votes:
                    metadata["votes"] = votes.get_text(strip=True)
            return metadata
        except Exception:
            pass
        return None

    def _extract_reddit(self, url: str, html: str, text: str) -> Optional[Dict[str, Any]]:
        if "reddit.com" not in url.lower():
            return None
        try:
            metadata = {"type": "reddit_post"}
            if "/r/" in url:
                parts = url.split("/r/")
                if len(parts) > 1:
                    metadata["subreddit"] = parts[1].split("/")[0]
            return metadata
        except Exception:
            pass
        return None

    async def run(self, intent: RIntent) -> List[EPacket]:
        logger.info(f"LF: query='{intent.query}' mode={intent.mode} max_results={intent.max_results}")
        urls = await self.discover(intent)
        stats = {"discovered": len(urls), "fetched": 0, "extracted": 0, "filtered_quality": 0, "filtered_duplicate": 0, "filtered_freshness": 0, "filtered_domain": 0, "final": 0}
        logger.debug(f"Discovered {len(urls)} URLs")

        profiles = self._detect_profile(intent)
        profile_allow = set()
        profile_deny = set()
        for pname in profiles:
            profile = self._PROFILES[pname]
            profile_allow.update(profile.allow_domains)
            profile_deny.update(profile.deny_domains)
        allow_domains = set(intent.allow_domains or []) | profile_allow
        deny_domains = set(intent.deny_domains or []) | profile_deny

        def allowed(u: str) -> bool:
            d = self._ext_domain(u)
            if allow_domains:
                if not any(d.endswith(ad) if ad.startswith(".") else d == ad or d.endswith(f".{ad}") for ad in allow_domains):
                    stats["filtered_domain"] += 1; return False
            if deny_domains:
                if any(d.endswith(dd) if dd.startswith(".") else d == dd or d.endswith(f".{dd}") for dd in deny_domains):
                    stats["filtered_domain"] += 1; return False
            return True
        urls = [u for u in urls if allowed(u)]

        fetched = await self.fetch_many(urls)
        now = datetime.now(timezone.utc)
        packets, selected, seen = [], [], set()

        for fr in fetched:
            if not fr.get("html"): continue
            stats["fetched"] += 1
            if intent.progress_callback:
                intent.progress_callback("extracting", stats["fetched"], len(fetched))
            
            text, pub_at, title, metadata, images, tables, language, content_type, sentiment, summary = self.extract(fr["url"], fr["html"], fr.get("content_type"))
            
            if not text or len(text.strip()) < self.config.min_text_length: continue
            stats["extracted"] += 1
            
            if intent.content_types and content_type:
                if not any(ct in content_type for ct in intent.content_types):
                    continue
            
            if intent.languages and language:
                if language not in intent.languages:
                    continue
            
            if len(text) > self.config.max_chars: text = text[:self.config.max_chars] + "\n[...clipped...]"
            sha = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

            if intent.freshness_days and pub_at:
                pub = pub_at if pub_at.tzinfo else pub_at.replace(tzinfo=timezone.utc)
                if pub < now - timedelta(days=intent.freshness_days): stats["filtered_freshness"] += 1; continue
            
            domain = self._ext_domain(fr["url"])
            profiles = self._detect_profile(intent)
            score, breakdown = self._calc_score(intent.query, domain, title or "", text, pub_at, now, selected, profiles)
            
            if not self._validate(text, score, sha, seen):
                if sha and sha in seen: stats["filtered_duplicate"] += 1
                else: stats["filtered_quality"] += 1
                continue
            
            if intent.need_summary and not summary:
                summary = self._summarize(text)
            
            site_extractors = [
                self._extract_wikipedia,
                self._extract_github,
                self._extract_stackoverflow,
                self._extract_reddit
            ]
            for ext in site_extractors:
                try:
                    ext_result = ext(fr["url"], fr["html"], text)
                    if ext_result:
                        metadata.update(ext_result)
                except Exception as e:
                    logger.debug(f"Site extractor failed: {e}")
            
            for ext in self.config.extractors:
                try:
                    ext_result = ext(fr["url"], fr["html"], text)
                    if ext_result:
                        metadata["custom_extractor"] = ext_result
                except Exception as e:
                    logger.debug(f"Custom extractor failed: {e}")
            
            packets.append(EPacket(url=fr["url"], domain=domain, title=title, text=text, published_at=pub_at, fetched_at=fr["fetched_at"], mode=intent.mode, score=score, score_breakdown=breakdown, content_sha256=sha, quotes=self._ext_quotes(text) if intent.need_quotes else [], metadata=metadata, images=images, tables=tables, language=language, content_type=content_type, sentiment=sentiment, summary=summary))

        packets.sort(key=lambda p: p.score, reverse=True)
        final = []
        for p in packets:
            if self._is_dup(p.content_sha256, seen): stats["filtered_duplicate"] += 1; continue
            if not self._validate(p.text, p.score, p.content_sha256, seen): stats["filtered_quality"] += 1; continue
            final.append(p)
            if p.content_sha256: seen.add(p.content_sha256)
            selected.append(p.text)
            self._cache_put(p.url, p.content_sha256 or "", p.fetched_at, p.published_at, p.title, p.text)
            if len(final) >= intent.max_results: break

        stats["final"] = len(final)
        logger.info(f"LF complete: {stats['final']} packets (discovered={stats['discovered']}, fetched={stats['fetched']}, extracted={stats['extracted']}, filtered_quality={stats['filtered_quality']}, filtered_duplicate={stats['filtered_duplicate']}, filtered_freshness={stats['filtered_freshness']}, filtered_domain={stats['filtered_domain']})")
        return final

    async def run_stream(self, intent: RIntent) -> AsyncGenerator[EPacket, None]:
        logger.info(f"LF stream: query='{intent.query}' mode={intent.mode} max_results={intent.max_results}")
        urls = await self.discover(intent)
        if intent.progress_callback:
            intent.progress_callback("discovered", len(urls), len(urls))
        
        profiles = self._detect_profile(intent)
        profile_allow = set()
        profile_deny = set()
        for pname in profiles:
            profile = self._PROFILES[pname]
            profile_allow.update(profile.allow_domains)
            profile_deny.update(profile.deny_domains)
        allow_domains = set(intent.allow_domains or []) | profile_allow
        deny_domains = set(intent.deny_domains or []) | profile_deny
        
        def allowed(u: str) -> bool:
            d = self._ext_domain(u)
            if allow_domains:
                if not any(d.endswith(ad) if ad.startswith(".") else d == ad or d.endswith(f".{ad}") for ad in allow_domains):
                    return False
            if deny_domains:
                if any(d.endswith(dd) if dd.startswith(".") else d == dd or d.endswith(f".{dd}") for dd in deny_domains):
                    return False
            return True
        urls = [u for u in urls if allowed(u)]
        
        fetched = await self.fetch_many(urls)
        now = datetime.now(timezone.utc)
        selected, seen = [], set()
        packets_processed = 0
        
        for fr in fetched:
            if not fr.get("html"): continue
            text, pub_at, title, metadata, images, tables, language, content_type, sentiment, summary = self.extract(fr["url"], fr["html"], fr.get("content_type"))
            if not text or len(text.strip()) < self.config.min_text_length: continue
            if len(text) > self.config.max_chars: text = text[:self.config.max_chars] + "\n[...clipped...]"
            sha = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
            if intent.freshness_days and pub_at:
                pub = pub_at if pub_at.tzinfo else pub_at.replace(tzinfo=timezone.utc)
                if pub < now - timedelta(days=intent.freshness_days): continue
            domain = self._ext_domain(fr["url"])
            profiles = self._detect_profile(intent)
            score, breakdown = self._calc_score(intent.query, domain, title or "", text, pub_at, now, selected, profiles)
            if not self._validate(text, score, sha, seen): continue
            if sha and sha in seen: continue
            if intent.need_summary and not summary:
                summary = self._summarize(text)
            packet = EPacket(url=fr["url"], domain=domain, title=title, text=text, published_at=pub_at, fetched_at=fr["fetched_at"], mode=intent.mode, score=score, score_breakdown=breakdown, content_sha256=sha, quotes=self._ext_quotes(text) if intent.need_quotes else [], metadata=metadata, images=images, tables=tables, language=language, content_type=content_type, sentiment=sentiment, summary=summary)
            seen.add(sha)
            selected.append(text)
            self._cache_put(packet.url, packet.content_sha256 or "", packet.fetched_at, packet.published_at, packet.title, packet.text)
            packets_processed += 1
            if intent.progress_callback:
                intent.progress_callback("processed", packets_processed, len(fetched))
            yield packet
            if packets_processed >= intent.max_results: break

    def run_sync(self, intent: RIntent) -> List[EPacket]: return asyncio.run(self.run(intent))

    async def run_batch(self, intents: List[RIntent]) -> List[List[EPacket]]:
        results = []
        for intent in intents:
            packets = await self.run(intent)
            results.append(packets)
        return results

    def get_metrics(self) -> Dict[str, Any]:
        latencies = self._metrics.get("latencies", [])
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        requests = self._metrics.get("requests", 0)
        success = self._metrics.get("success", 0)
        failures = self._metrics.get("failures", 0)
        success_rate = success / requests if requests > 0 else 0.0
        cache_hits = self._metrics.get("cache_hits", 0)
        cache_misses = self._metrics.get("cache_misses", 0)
        cache_hit_rate = cache_hits / (cache_hits + cache_misses) if (cache_hits + cache_misses) > 0 else 0.0
        return {"requests": requests, "success": success, "failures": failures, "success_rate": success_rate, "avg_latency": avg_latency, "cache_hits": cache_hits, "cache_misses": cache_misses, "cache_hit_rate": cache_hit_rate}

    def health_check(self) -> Dict[str, bool]:
        health = {"cache": False, "discovery": False}
        try:
            if self._conn:
                self._conn.execute("SELECT 1").fetchone()
                health["cache"] = True
        except Exception:
            pass
        try:
            with DDGS() as ddgs:
                list(ddgs.text("test", max_results=1))
                health["discovery"] = True
        except Exception:
            pass
        return health

    def export_to_json(self, packets: List[EPacket]) -> str:
        import json
        from dataclasses import asdict
        return json.dumps([asdict(p) for p in packets], default=str, indent=2)

    def export_to_csv(self, packets: List[EPacket]) -> str:
        import csv
        from io import StringIO
        output = StringIO()
        if not packets:
            return ""
        writer = csv.DictWriter(output, fieldnames=["url", "domain", "title", "score", "published_at", "fetched_at", "language", "content_type"])
        writer.writeheader()
        for p in packets:
            writer.writerow({"url": p.url, "domain": p.domain, "title": p.title or "", "score": p.score, "published_at": p.published_at.isoformat() if p.published_at else "", "fetched_at": p.fetched_at.isoformat(), "language": p.language or "", "content_type": p.content_type or ""})
        return output.getvalue()

    def export_to_markdown(self, packets: List[EPacket]) -> str:
        md = "# LiveFetch Results\n\n"
        for i, p in enumerate(packets, 1):
            md += f"## {i}. {p.title or 'Untitled'}\n\n"
            md += f"**URL:** {p.url}\n\n"
            md += f"**Domain:** {p.domain} | **Score:** {p.score:.3f}\n\n"
            if p.published_at:
                md += f"**Published:** {p.published_at.strftime('%Y-%m-%d')}\n\n"
            if p.language:
                md += f"**Language:** {p.language}\n\n"
            if p.quotes:
                md += "**Key Quotes:**\n\n"
                for quote in p.quotes:
                    md += f"- {quote}\n\n"
            md += f"**Content:**\n\n{p.text[:1000]}...\n\n---\n\n"
        return md

    def export_to_html(self, packets: List[EPacket]) -> str:
        html = "<!DOCTYPE html><html><head><title>LiveFetch Results</title><style>body{font-family:Arial,sans-serif;max-width:1200px;margin:0 auto;padding:20px;}h1{color:#333;}h2{border-bottom:2px solid #333;padding-bottom:10px;}p{margin:10px 0;}</style></head><body><h1>LiveFetch Results</h1>"
        for i, p in enumerate(packets, 1):
            html += f"<h2>{i}. {p.title or 'Untitled'}</h2>"
            html += f"<p><strong>URL:</strong> <a href='{p.url}'>{p.url}</a></p>"
            html += f"<p><strong>Domain:</strong> {p.domain} | <strong>Score:</strong> {p.score:.3f}</p>"
            if p.published_at:
                html += f"<p><strong>Published:</strong> {p.published_at.strftime('%Y-%m-%d')}</p>"
            if p.quotes:
                html += "<p><strong>Key Quotes:</strong></p><ul>"
                for quote in p.quotes:
                    html += f"<li>{quote}</li>"
                html += "</ul>"
            html += f"<p><strong>Content:</strong></p><p>{p.text[:1000]}...</p><hr>"
        html += "</body></html>"
        return html

    def fetch(self, query: str, mode: Optional[Mode] = None, max_results: int = 8, freshness_days: Optional[int] = 14, need_quotes: bool = True, allow_domains: Optional[List[str]] = None, deny_domains: Optional[List[str]] = None, profile: Optional[str] = None, auto_detect_profile: bool = True) -> List[EPacket]:
        try:
            if mode is None: mode = self._detect_mode(query)
            intent = RIntent(query=query, mode=mode, max_results=max_results, freshness_days=freshness_days, need_quotes=need_quotes, allow_domains=allow_domains, deny_domains=deny_domains, profile=profile, auto_detect_profile=auto_detect_profile)
            return self.run_sync(intent)
        finally:
            if self._auto_close: self.close()

    def _detect_mode(self, query: str) -> Mode:
        q = query.strip().lower()
        if q.startswith(("http://", "https://", "feed://", "rss://")): return "rss"
        if any(x in q for x in ["/rss", "/feed", ".xml", "rss", "atom"]): return "rss"
        return "news"

    def _detect_profile(self, intent: RIntent) -> List[str]:
        if intent.profile:
            return [intent.profile] if intent.profile in self._PROFILES else []
        if not intent.auto_detect_profile:
            return []
        q_lower = intent.query.lower()
        matches = []
        for name, profile in self._PROFILES.items():
            if any(kw in q_lower for kw in profile.keywords):
                matches.append(name)
        return matches

    def _apply_profile_query(self, intent: RIntent) -> str:
        profiles = self._detect_profile(intent)
        if not profiles:
            return intent.query
        query = intent.query
        modifiers = []
        for pname in profiles:
            profile = self._PROFILES[pname]
            modifiers.extend(profile.query_modifiers)
        if modifiers:
            site_mods = [m for m in modifiers if m.startswith("site:")]
            other_mods = [m for m in modifiers if not m.startswith("site:")]
            if site_mods:
                query = f"{query} ({' OR '.join(site_mods)})"
            if other_mods:
                query = f"{query} {' '.join(other_mods[:2])}"
        return query


class QueryQueue:
    def __init__(self, lf: "LF"):
        self.lf = lf
        self.queue: List[Tuple[int, RIntent]] = []
    
    def add(self, intent: RIntent, priority: int = 0) -> None:
        self.queue.append((priority, intent))
        self.queue.sort(key=lambda x: x[0], reverse=True)
    
    async def process(self) -> List[List[EPacket]]:
        results = []
        for _, intent in self.queue:
            packets = await self.lf.run(intent)
            results.append(packets)
        return results
    
    def clear(self) -> None:
        self.queue.clear()


def fetch(query: str, mode: Optional[Mode] = None, max_results: int = 8, freshness_days: Optional[int] = 14, need_quotes: bool = True, allow_domains: Optional[List[str]] = None, deny_domains: Optional[List[str]] = None, profile: Optional[str] = None, auto_detect_profile: bool = True, cache_path: str = "livefetch.sqlite") -> List[EPacket]:
    with LF(LFConfig(cache_path=cache_path)) as lf:
        return lf.fetch(query=query, mode=mode, max_results=max_results, freshness_days=freshness_days, need_quotes=need_quotes, allow_domains=allow_domains, deny_domains=deny_domains, profile=profile, auto_detect_profile=auto_detect_profile)


# Backward compatibility aliases
RetrievalIntent = RIntent
EvidencePacket = EPacket
LiveFetchConfig = LFConfig
LiveFetch = LF
LiveFetchError = LFError
DiscoveryError = DiscError
FetchError = FetchErr
ExtractionError = ExtError


if __name__ == "__main__":
    import json
    from dataclasses import asdict
    packets = fetch("latest EU AI Act enforcement timeline", max_results=5, freshness_days=30)
    print(json.dumps([asdict(p) for p in packets], default=str, indent=2))
