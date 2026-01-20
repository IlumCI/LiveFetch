"""Type stubs for LiveFetch."""

from typing import Any, AsyncGenerator, Callable, Dict, List, Literal, Optional, Tuple
from datetime import datetime

Mode = Literal["web", "news", "rss"]

class RIntent:
    query: str
    mode: Mode
    max_results: int
    freshness_days: Optional[int]
    need_quotes: bool
    allow_domains: Optional[List[str]]
    deny_domains: Optional[List[str]]
    profile: Optional[str]
    auto_detect_profile: bool
    progress_callback: Optional[Callable[[str, int, int], None]]
    content_types: Optional[List[str]]
    languages: Optional[List[str]]
    need_summary: bool
    def __init__(self, query: str, mode: Mode = ..., max_results: int = ..., freshness_days: Optional[int] = ..., need_quotes: bool = ..., allow_domains: Optional[List[str]] = ..., deny_domains: Optional[List[str]] = ..., profile: Optional[str] = ..., auto_detect_profile: bool = ..., progress_callback: Optional[Callable[[str, int, int], None]] = ..., content_types: Optional[List[str]] = ..., languages: Optional[List[str]] = ..., need_summary: bool = ...) -> None: ...

class EPacket:
    url: str
    domain: str
    title: Optional[str]
    text: str
    published_at: Optional[datetime]
    fetched_at: datetime
    mode: str
    score: float
    score_breakdown: Dict[str, float]
    content_sha256: Optional[str]
    quotes: List[str]
    metadata: Dict[str, Any]
    images: List[Dict[str, str]]
    tables: List[Dict[str, Any]]
    language: Optional[str]
    content_type: Optional[str]
    sentiment: Optional[float]
    summary: Optional[str]
    def __init__(self, url: str, domain: str, title: Optional[str], text: str, published_at: Optional[datetime], fetched_at: datetime, mode: str, score: float = ..., score_breakdown: Dict[str, float] = ..., content_sha256: Optional[str] = ..., quotes: List[str] = ..., metadata: Dict[str, Any] = ..., images: List[Dict[str, str]] = ..., tables: List[Dict[str, Any]] = ..., language: Optional[str] = ..., content_type: Optional[str] = ..., sentiment: Optional[float] = ..., summary: Optional[str] = ...) -> None: ...

class LFConfig:
    cache_path: str
    max_results: int
    max_concurrency: int
    timeout_seconds: float
    max_chars: int
    freshness_half_life: float
    min_text_length: int
    min_score: float
    weights: Dict[str, float]
    user_agent: str
    max_retries: int
    backoff_base: float
    backoff_max: float
    jitter: bool
    circuit_breaker_threshold: int
    circuit_breaker_timeout: float
    rate_limit_per_domain: Dict[str, float]
    default_rate_limit: float
    user_agents: List[str]
    proxy: Optional[str]
    proxy_rotation: List[str]
    cache_ttl_days: Optional[int]
    max_file_size: Optional[int]
    extractors: List[Callable]
    def __init__(self, cache_path: str = ..., max_results: int = ..., max_concurrency: int = ..., timeout_seconds: float = ..., max_chars: int = ..., freshness_half_life: float = ..., min_text_length: int = ..., min_score: float = ..., weights: Dict[str, float] = ..., user_agent: str = ..., max_retries: int = ..., backoff_base: float = ..., backoff_max: float = ..., jitter: bool = ..., circuit_breaker_threshold: int = ..., circuit_breaker_timeout: float = ..., rate_limit_per_domain: Dict[str, float] = ..., default_rate_limit: float = ..., user_agents: List[str] = ..., proxy: Optional[str] = ..., proxy_rotation: List[str] = ..., cache_ttl_days: Optional[int] = ..., max_file_size: Optional[int] = ..., extractors: List[Callable] = ...) -> None: ...

class LF:
    def __init__(self, config: Optional[LFConfig] = ..., auto_close: bool = ...) -> None: ...
    def __enter__(self) -> "LF": ...
    def __exit__(self, *args: Any) -> None: ...
    def close(self) -> None: ...
    def discover(self, intent: RIntent) -> List[str]: ...
    async def run(self, intent: RIntent) -> List[EPacket]: ...
    async def run_stream(self, intent: RIntent) -> AsyncGenerator[EPacket, None]: ...
    def run_sync(self, intent: RIntent) -> List[EPacket]: ...
    async def run_batch(self, intents: List[RIntent]) -> List[List[EPacket]]: ...
    def fetch(self, query: str, mode: Optional[Mode] = ..., max_results: int = ..., freshness_days: Optional[int] = ..., need_quotes: bool = ..., allow_domains: Optional[List[str]] = ..., deny_domains: Optional[List[str]] = ..., profile: Optional[str] = ..., auto_detect_profile: bool = ...) -> List[EPacket]: ...
    def get_metrics(self) -> Dict[str, Any]: ...
    def health_check(self) -> Dict[str, bool]: ...
    def get_cache_stats(self) -> Dict[str, Any]: ...
    def warm_cache(self, urls: Optional[List[str]] = ...) -> None: ...
    def export_to_json(self, packets: List[EPacket]) -> str: ...
    def export_to_csv(self, packets: List[EPacket]) -> str: ...
    def export_to_markdown(self, packets: List[EPacket]) -> str: ...
    def export_to_html(self, packets: List[EPacket]) -> str: ...

class QueryQueue:
    def __init__(self, lf: LF) -> None: ...
    def add(self, intent: RIntent, priority: int = ...) -> None: ...
    async def process(self) -> List[List[EPacket]]: ...
    def clear(self) -> None: ...

def fetch(query: str, mode: Optional[Mode] = ..., max_results: int = ..., freshness_days: Optional[int] = ..., need_quotes: bool = ..., allow_domains: Optional[List[str]] = ..., deny_domains: Optional[List[str]] = ..., profile: Optional[str] = ..., auto_detect_profile: bool = ..., cache_path: str = ...) -> List[EPacket]: ...

RetrievalIntent = RIntent
EvidencePacket = EPacket
LiveFetchConfig = LFConfig
LiveFetch = LF
LiveFetchError = Exception
DiscoveryError = Exception
FetchError = Exception
ExtractionError = Exception
