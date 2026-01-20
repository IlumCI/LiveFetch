#!/usr/bin/env python3
"""LiveFetch CLI tool."""

import argparse
import json
import sys
from typing import Optional

from main import LF, LFConfig, RIntent, Mode


def main():
    parser = argparse.ArgumentParser(description="LiveFetch - Keyless web retrieval")
    parser.add_argument("query", help="Search query or RSS URL")
    parser.add_argument("--mode", choices=["web", "news", "rss"], default=None, help="Discovery mode")
    parser.add_argument("--max-results", type=int, default=8, help="Maximum results")
    parser.add_argument("--freshness-days", type=int, default=None, help="Freshness filter in days")
    parser.add_argument("--need-quotes", action="store_true", default=True, help="Extract quotes")
    parser.add_argument("--no-quotes", action="store_false", dest="need_quotes", help="Don't extract quotes")
    parser.add_argument("--allow-domains", nargs="+", help="Allowed domains")
    parser.add_argument("--deny-domains", nargs="+", help="Denied domains")
    parser.add_argument("--profile", help="Profile name")
    parser.add_argument("--no-auto-detect-profile", action="store_false", dest="auto_detect_profile", help="Disable auto profile detection")
    parser.add_argument("--export", choices=["json", "csv", "markdown", "html"], default="json", help="Export format")
    parser.add_argument("--cache-path", default="livefetch.sqlite", help="Cache database path")
    parser.add_argument("--output", help="Output file (default: stdout)")
    parser.add_argument("--need-summary", action="store_true", help="Generate summaries")
    parser.add_argument("--content-types", nargs="+", help="Filter by content types")
    parser.add_argument("--languages", nargs="+", help="Filter by languages")
    
    args = parser.parse_args()
    
    config = LFConfig(cache_path=args.cache_path)
    intent = RIntent(
        query=args.query,
        mode=args.mode or "news",
        max_results=args.max_results,
        freshness_days=args.freshness_days,
        need_quotes=args.need_quotes,
        allow_domains=args.allow_domains,
        deny_domains=args.deny_domains,
        profile=args.profile,
        auto_detect_profile=args.auto_detect_profile,
        need_summary=args.need_summary,
        content_types=args.content_types,
        languages=args.languages
    )
    
    with LF(config) as lf:
        packets = lf.run_sync(intent)
        
        if args.export == "json":
            output = lf.export_to_json(packets)
        elif args.export == "csv":
            output = lf.export_to_csv(packets)
        elif args.export == "markdown":
            output = lf.export_to_markdown(packets)
        elif args.export == "html":
            output = lf.export_to_html(packets)
        else:
            output = ""
        
        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output)
        else:
            print(output)


if __name__ == "__main__":
    main()
