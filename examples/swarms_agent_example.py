"""Swarms Agent example using LiveFetch for Ukraine War data with LLM analysis."""

from datetime import datetime, timezone
from typing import List
from dotenv import load_dotenv

from main import LiveFetch, RetrievalIntent, EvidencePacket
from loguru import logger
from swarms import Agent

# Load environment variables
load_dotenv()


def fetch_ukraine_data(max_results: int = 8) -> List[EvidencePacket]:
    """Fetch Ukraine War news using LiveFetch."""
    lf = LiveFetch()
    intent = RetrievalIntent(
        query="Ukraine war latest news military developments",
        mode="news",
        max_results=max_results,
        freshness_days=7,
        need_quotes=True
    )
    return lf.run_sync(intent)


def format_evidence_for_llm(packets: List[EvidencePacket]) -> str:
    """Format evidence packets as context for LLM."""
    context = "## Evidence Sources\n\n"
    for i, p in enumerate(sorted(packets, key=lambda x: x.score, reverse=True), 1):
        context += f"### Source {i}: {p.title or 'Untitled'}\n"
        context += f"**URL:** {p.url}\n**Domain:** {p.domain}\n**Score:** {p.score:.3f}\n"
        if p.published_at:
            context += f"**Published:** {p.published_at.strftime('%Y-%m-%d')}\n"
        if p.quotes:
            context += "\n**Key Quotes:**\n"
            for quote in p.quotes[:3]:
                context += f"- {quote}\n"
        context += f"\n**Content:**\n{p.text[:800]}\n\n---\n\n"
    return context


def generate_report(output_file: str = "ukraine_war_report_swarms.md") -> str:
    """Generate Ukraine War report using Swarms Agent with LiveFetch data."""
    logger.info("Fetching Ukraine War data with LiveFetch...")
    packets = fetch_ukraine_data(max_results=8)
    
    if not packets:
        return "# Report\n\nNo sources found.\n"
    
    logger.info(f"Retrieved {len(packets)} evidence packets")
    
    # Format evidence for Swarms Agent
    evidence_context = format_evidence_for_llm(packets)
    
    # Initialize Swarms Agent with LLM
    logger.info("Using Swarms Agent with LLM to analyze evidence and generate report...")
    agent = Agent(
        agent_name="ukraine-war-analyst",
        system_prompt="You are an expert intelligence analyst specializing in conflict analysis. Provide factual, well-sourced analysis.",
        model_name="gpt-4o-mini",
        max_loops=1,
        temperature=0.3
    )
    
    # Create task with evidence
    task = f"""Analyze the following Ukraine War evidence and create a comprehensive markdown report.

Include:
1. Executive summary of key developments
2. Analysis of trends and patterns
3. Key findings from each source
4. Implications and context

Evidence:
{evidence_context}

Generate a well-structured markdown report with proper citations."""
    
    # Run Swarms Agent - this uses LLM
    result = agent.run(task)
    
    # Extract response (Swarms Agent returns the response directly)
    analysis = str(result) if result else ""
    
    # Build markdown report
    header = f"# Ukraine War Intelligence Report\n\n"
    header += f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
    header += f"**Sources Analyzed:** {len(packets)}\n\n"
    header += "---\n\n"
    
    report = header + analysis
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(report)
    
    logger.info(f"Report saved to {output_file}")
    return report


if __name__ == "__main__":
    report = generate_report("ukraine_war_report_swarms.md")
    print(f"Report generated: {len(report)} characters")
