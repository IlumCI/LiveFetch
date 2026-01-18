from tools.DCC.main import LiveFetch, RetrievalIntent

intent = RetrievalIntent(
    query="latest Ukraine war news",
    mode="news",  # or "web", "rss"
    max_results=5,
    freshness_days=30,
    need_quotes=True,
)

lf = LiveFetch()
packets = lf.run_sync(intent)  # or: await lf.run(intent)