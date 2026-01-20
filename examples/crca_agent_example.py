"""CRCAAgent example using LiveFetch for Ukraine War data with LLM analysis."""

from datetime import datetime, timezone
from typing import List
from dotenv import load_dotenv

from main import LiveFetch, RetrievalIntent, EvidencePacket
from loguru import logger
from CRCA import CRCAAgent

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


def generate_report(output_file: str = "ukraine_war_report.md") -> str:
    """Generate Ukraine War report using CRCAAgent with LiveFetch data."""
    logger.info("Fetching Ukraine War data with LiveFetch...")
    packets = fetch_ukraine_data(max_results=8)
    
    if not packets:
        return "# Report\n\nNo sources found.\n"
    
    logger.info(f"Retrieved {len(packets)} evidence packets")
    
    # Format evidence for CRCAAgent
    evidence_context = format_evidence_for_llm(packets)
    
    # Initialize CRCAAgent with LLM
    logger.info("Using CRCAAgent with LLM to analyze evidence and generate report...")
    agent = CRCAAgent(
        model_name="gpt-4o-mini",
        max_loops=3,
        agent_max_loops=3
    )
    
    # Use CRCAAgent's run() method with task string - this uses LLM
    task = f"""Analyze the following Ukraine War evidence and create a comprehensive markdown report.

Include:
1. Executive summary of key developments
2. Analysis of trends and patterns using causal reasoning
3. Key findings from each source
4. Implications and context

Evidence:
{evidence_context}

Generate a well-structured markdown report with proper citations and causal analysis."""
    
    # Run CRCAAgent - this uses LLM for causal analysis
    result = agent.run(task=task)
    
    # Debug: log result structure to identify full LLM response
    logger.debug(f"CRCAAgent result type: {type(result)}")
    if isinstance(result, dict):
        logger.debug(f"CRCAAgent result keys: {list(result.keys())}")
        # Log all string values to identify potential full responses
        for key, value in result.items():
            if isinstance(value, str):
                logger.debug(f"  {key}: {len(value)} chars")
    
    # Try to get full LLM response if available
    # CRCAAgent may expose the full response in various fields
    full_response = None
    if isinstance(result, dict):
        # Check common fields that might contain full LLM response
        full_response = (
            result.get("response") or 
            result.get("output") or 
            result.get("content") or
            result.get("full_response") or
            result.get("llm_response") or
            result.get("agent_response") or
            result.get("analysis") or
            result.get("final_answer") or
            result.get("answer")
        )
        # Check if result itself is a string or has a string value
        if not full_response:
            for key, value in result.items():
                if isinstance(value, str) and len(value) > 500:  # Likely full response
                    full_response = value
                    logger.info(f"Found full response in field: {key}")
                    break
    elif isinstance(result, str):
        # Result is directly a string (full response)
        full_response = result
    
    # If we have a full response, use it; otherwise construct from fields
    if full_response:
        analysis = str(full_response)
    else:
        # Extract all available fields and construct comprehensive report
        if isinstance(result, dict):
            causal_analysis = result.get("causal_analysis", "")
            intervention_planning = result.get("intervention_planning", "")
            optimal_solution = result.get("optimal_solution", "")
            causal_strength = result.get("causal_strength_assessment", "")
            counterfactuals = result.get("counterfactual_scenarios", [])
        else:
            causal_analysis = str(result)
            intervention_planning = ""
            optimal_solution = ""
            causal_strength = ""
            counterfactuals = []
        
        # Construct comprehensive report from all available fields
        analysis_parts = []
        if causal_analysis:
            analysis_parts.append(causal_analysis)
        if intervention_planning:
            analysis_parts.append(f"\n## Intervention Planning\n\n{intervention_planning}")
        if causal_strength:
            analysis_parts.append(f"\n## Causal Strength Assessment\n\n{causal_strength}")
        if optimal_solution:
            analysis_parts.append(f"\n## Optimal Solution\n\n{optimal_solution}")
        if counterfactuals:
            analysis_parts.append("\n## Counterfactual Scenarios\n")
            for i, scenario in enumerate(counterfactuals[:5], 1):
                if isinstance(scenario, dict):
                    name = scenario.get("scenario_name") or scenario.get("name", f"Scenario {i}")
                    reasoning = scenario.get("reasoning", "")
                else:
                    name = getattr(scenario, "name", f"Scenario {i}")
                    reasoning = getattr(scenario, "reasoning", "")
                analysis_parts.append(f"\n### Scenario {i}: {name}\n{reasoning}")
        
        analysis = "\n".join(analysis_parts) if analysis_parts else str(result)
    
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
    report = generate_report("ukraine_war_report.md")
    print(f"Report generated: {len(report)} characters")
