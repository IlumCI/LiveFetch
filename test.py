from datetime import datetime, timezone
from dotenv import load_dotenv
import httpx
import asyncio
from main import LiveFetch, RetrievalIntent
from CRCA import CRCAAgent

load_dotenv()

async def fetch_dexscreener_tokens():
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            tokens = []
            seen_symbols = set()
            
            search_queries = ["ethereum", "solana", "base", "arbitrum", "polygon"]
            
            for query in search_queries:
                if len(tokens) >= 10:
                    break
                    
                search_url = f"https://api.dexscreener.com/latest/dex/search?q={query}"
                r = await client.get(search_url)
                
                if r.status_code == 200:
                    data = r.json()
                    if not isinstance(data, dict):
                        continue
                    
                    pairs = data.get("pairs", [])
                    if not isinstance(pairs, list):
                        continue
                    
                    for pair in pairs:
                        if len(tokens) >= 10:
                            break
                        
                        if not isinstance(pair, dict):
                            continue
                        
                        base_token = pair.get("baseToken", {})
                        if not isinstance(base_token, dict):
                            continue
                        
                        symbol = base_token.get("symbol", "").upper()
                        if not symbol or symbol in seen_symbols:
                            continue
                        
                        seen_symbols.add(symbol)
                        price_change = pair.get("priceChange", {})
                        volume = pair.get("volume", {})
                        liquidity = pair.get("liquidity", {})
                        
                        price_usd = pair.get("priceUsd")
                        if price_usd:
                            try:
                                price_val = float(str(price_usd).replace(",", ""))
                            except (ValueError, TypeError):
                                price_val = 0.0
                        else:
                            price_val = 0.0
                        
                        token_info = {
                            "name": base_token.get("name", "Unknown"),
                            "symbol": symbol,
                            "price": str(price_val) if price_val > 0 else "0",
                            "priceChange24h": float(price_change.get("h24", 0)) if isinstance(price_change, dict) else 0.0,
                            "volume24h": float(volume.get("h24", 0)) if isinstance(volume, dict) else 0.0,
                            "liquidity": float(liquidity.get("usd", 0)) if isinstance(liquidity, dict) else 0.0,
                            "fdv": float(pair.get("fdv", 0)) if pair.get("fdv") else 0.0,
                            "chainId": pair.get("chainId", "ethereum"),
                            "url": pair.get("url") or f"https://dexscreener.com/{pair.get('chainId', 'ethereum')}/{pair.get('pairAddress', '')}"
                        }
                        tokens.append(token_info)
            
            if len(tokens) < 10:
                try:
                    boosts_url = "https://api.dexscreener.com/token-boosts/top/v1"
                    r = await client.get(boosts_url)
                    if r.status_code == 200:
                        boost = r.json()
                        if isinstance(boost, dict):
                            chain_id = boost.get("chainId", "ethereum")
                            pair_address = boost.get("pairAddress")
                            if pair_address:
                                pair_url = f"https://api.dexscreener.com/latest/dex/pairs/{chain_id}/{pair_address}"
                                pair_r = await client.get(pair_url)
                                if pair_r.status_code == 200:
                                    pair_data = pair_r.json()
                                    if isinstance(pair_data, dict):
                                        pairs = pair_data.get("pairs", [])
                                        if isinstance(pairs, list) and pairs:
                                            pair = pairs[0]
                                            base_token = pair.get("baseToken", {})
                                            if isinstance(base_token, dict):
                                                symbol = base_token.get("symbol", "").upper()
                                                if symbol and symbol not in seen_symbols:
                                                    seen_symbols.add(symbol)
                                                    price_change = pair.get("priceChange", {})
                                                    volume = pair.get("volume", {})
                                                    liquidity = pair.get("liquidity", {})
                                                    token_info = {
                                                        "name": base_token.get("name", "Unknown"),
                                                        "symbol": symbol,
                                                        "price": str(pair.get("priceUsd", "0")),
                                                        "priceChange24h": float(price_change.get("h24", 0)) if isinstance(price_change, dict) else 0.0,
                                                        "volume24h": float(volume.get("h24", 0)) if isinstance(volume, dict) else 0.0,
                                                        "liquidity": float(liquidity.get("usd", 0)) if isinstance(liquidity, dict) else 0.0,
                                                        "fdv": float(pair.get("fdv", 0)) if pair.get("fdv") else 0.0,
                                                        "chainId": chain_id,
                                                        "url": pair.get("url") or f"https://dexscreener.com/{chain_id}/{pair_address}"
                                                    }
                                                    tokens.append(token_info)
                except Exception:
                    pass
            
            return tokens
        except Exception as e:
            print(f"[!] Error fetching tokens: {e}")
            return []
    return []

tokens = asyncio.run(fetch_dexscreener_tokens())

lf = LiveFetch()
evidence = "## Top 10 DEX Token Analysis\n\n"

for i, token in enumerate(tokens[:10], 1):
    evidence += f"### Token {i}: {token['name']} ({token['symbol']})\n"
    evidence += f"Price: ${token['price']}\n"
    evidence += f"24h Change: {token['priceChange24h']:.2f}%\n"
    evidence += f"24h Volume: ${token['volume24h']:,.0f}\n"
    evidence += f"Liquidity: ${token['liquidity']:,.0f}\n"
    evidence += f"FDV: ${token['fdv']:,.0f}\n"
    evidence += f"Chain: {token['chainId']}\n"
    evidence += f"URL: {token['url']}\n\n"
    
    intent = RetrievalIntent(
        query=f"{token['symbol']} {token['name']} crypto token news price analysis trading",
        mode="news",
        max_results=10,
        profile="market",
        need_quotes=True,
        freshness_days=7
    )
    packets = lf.run_sync(intent)
    
    if packets:
        evidence += f"**Recent News & Media ({len(packets)} sources):**\n"
        for j, p in enumerate(packets[:5], 1):
            evidence += f"{j}. {p.title or 'Untitled'}\n"
            evidence += f"   Source: {p.domain} | Score: {p.score:.3f}\n"
            if p.metadata.get("market_data"):
                evidence += f"   Market Data: {p.metadata['market_data'][:100]}...\n"
            if p.quotes:
                evidence += f"   Key Quote: {p.quotes[0][:150]}...\n"
            evidence += f"   URL: {p.url}\n"
        evidence += "\n"

task = f"""Analyze these top 10 DEX tokens from Dexscreener and generate a comprehensive trading report using causal reasoning.

For each token, provide:
1. Direction: [BULLISH/BEARISH/NEUTRAL]
2. Buy Signal: [YES/NO] + Entry Price Range + Rationale
3. Sell Signal: [YES/NO] + Exit Price Target + Rationale
4. Expected Returns: [%] + PnL Estimate + Timeframe
5. Risk Assessment: [LOW/MEDIUM/HIGH] + Risk Factors
6. Causal Analysis: Identify causal relationships between price movements, volume, news sentiment, and market dynamics

Use structural causal modeling to understand why price movements occur, not just correlations.
Consider counterfactual scenarios: "What if volume increased 50%?" or "What if negative news emerged?"

{evidence}

Format as markdown with clear sections for each token. Include causal chains and counterfactual analysis."""

agent = CRCAAgent(
    model_name="gpt-4o-mini",
    max_loops=10,
    agent_max_loops=10,
    enable_batch_predict=False
)
result = agent.run(task=task)

full_response = None
if isinstance(result, dict):
    if "causal_analysis" in result:
        causal = result["causal_analysis"]
        full_response = f"## Causal Analysis\n\n{causal}\n\n"
    if "counterfactual_scenarios" in result:
        scenarios = result["counterfactual_scenarios"]
        full_response = (full_response or "") + "## Counterfactual Scenarios\n\n"
        for scenario in scenarios[:5]:
            if isinstance(scenario, dict):
                name = scenario.get("name") or scenario.get("scenario_name", "Unknown")
                reasoning = scenario.get("reasoning") or scenario.get("description", "")
            else:
                name = getattr(scenario, "name", "Unknown")
                reasoning = getattr(scenario, "reasoning", getattr(scenario, "description", ""))
            full_response += f"### {name}\n{reasoning}\n\n"
    if not full_response:
        full_response = (result.get("response") or result.get("output") or result.get("content") or 
                         result.get("full_response") or result.get("llm_response") or 
                         result.get("analysis") or result.get("final_answer") or result.get("answer"))
        if not full_response:
            for k, v in result.items():
                if isinstance(v, str) and len(v) > 500:
                    full_response = v
                    break
elif isinstance(result, str):
    full_response = result

report = f"# DEX Token Trading Analysis Report\n\n"
report += f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
report += f"Tokens Analyzed: {len(tokens)}\n\n"
report += "=" * 60 + "\n\n"
report += full_response if full_response else str(result)

with open("dex_trading_report.md", "w", encoding="utf-8") as f:
    f.write(report)

print(f"[+] Report generated: {len(report)} chars")
print(f"[*] Tokens analyzed: {len(tokens)}")
