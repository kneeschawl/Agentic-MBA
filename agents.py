import httpx
import json

# Local Ollama connection point
OLLAMA_URL = "http://localhost:11434/api/chat"
MODEL_NAME = "llama3.1:8b"  # Ensure this matches the 'ollama list' tag

# ==========================================
# AGENT SYSTEM PROMPTS (The Core Engine)
# ==========================================

PSYCHOLOGIST_SYSTEM_PROMPT = """
You are an elite Consumer Psychologist and Behavioral Economist specializing in retail purchase patterns.
Your job is to analyze Market Basket Association Rules and explain the invisible human behavior behind them.

You will be given a set of association rules (antecedents, consequents, support, confidence, lift).
For each rule, provide a behavioral analysis containing:
1. Cognitive Trigger: Why are these items bought together? (e.g., Ritualistic behavior, impulse, logical convenience, routine preparation).
2. Friction Level: Is this a high-consideration purchase or automatic habit?
3. The "Hook": A one-sentence psychological summary of the customer's mindset.

Format your output strictly as a JSON list matching this structure:
[
  {
    "antecedents": [...],
    "consequents": [...],
    "cognitive_trigger": "...",
    "friction_level": "Low/Medium/High",
    "hook": "..."
  }
]
Do NOT write any conversational intro or outro text. Output ONLY the raw JSON block.
"""

MERCHANDISER_SYSTEM_PROMPT = """
You are a brilliant Visual Merchandiser and Retail UX Architect. 
Your job is to take consumer psychology profiles and translate them into physical and digital store layouts.

You will be given the behavioral analysis JSON of association rules.
For each rule, provide an actionable merchandising strategy containing:
1. Spatial Placement: Where do these items go relative to each other? (e.g., eye-level cross-merchandising, physical distancing to force foot traffic, adjacent shelving).
2. Digital UI Strategy: How should a mobile app or website display these? (e.g., post-cart upsell, quick-add bundle).
3. Signage & Copy: A specific, persuasive sign or copy to place near the items (e.g., "Baking tonight? Don't forget the milk!").

Format your output strictly as a JSON list matching this structure:
[
  {
    "antecedents": [...],
    "consequents": [...],
    "spatial_placement": "...",
    "digital_ui_strategy": "...",
    "signage_copy": "..."
  }
]
Do NOT write any conversational intro or outro text. Output ONLY the raw JSON block.
"""

# ==========================================
# OLLAMA ORCHESTRATION FUNCTIONS
# ==========================================

# Create a robust timeout configuration (None means no limit/infinite patience)
robust_timeout = httpx.Timeout(
    connect=10.0,  # 10 seconds to establish the initial connection
    read=None,     # INFINITE wait time for Ollama to think and generate the output
    write=30.0,    # 30 seconds to send the payload
    pool=10.0      # 10 seconds to retrieve a connection from the pool
)

async def run_consumer_psychologist(rules_json: list) -> list:
    """Invokes the Consumer Psychologist agent to analyze raw market basket rules."""
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": PSYCHOLOGIST_SYSTEM_PROMPT},
            {"role": "user", "content": f"Analyze these rules:\n{json.dumps(rules_json)}"}
        ],
        "stream": False,
        "format": "json"
    }
    
    # --- FIXED: Use the robust timeout configuration ---
    async with httpx.AsyncClient(timeout=robust_timeout) as client:
        response = await client.post(OLLAMA_URL, json=payload)
        response_data = response.json()
        raw_content = response_data["message"]["content"]
        return json.loads(raw_content)


async def run_visual_merchandiser(psychology_insights: list) -> list:
    """Invokes the Visual Merchandiser agent to build layout strategies based on psychology."""
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": MERCHANDISER_SYSTEM_PROMPT},
            {"role": "user", "content": f"Design placement strategies for these profiles:\n{json.dumps(psychology_insights)}"}
        ],
        "stream": False,
        "format": "json"
    }
    
    # --- FIXED: Use the robust timeout configuration ---
    async with httpx.AsyncClient(timeout=robust_timeout) as client:
        response = await client.post(OLLAMA_URL, json=payload)
        response_data = response.json()
        raw_content = response_data["message"]["content"]
        return json.loads(raw_content)
    
# ==========================================
# EXECUTIVE COORDINATOR PROMPT
# ==========================================

REPORTER_SYSTEM_PROMPT = """
You are an elite Retail Consultant and Chief Strategy Officer (CSO). 
Your job is to synthesize raw basket analysis math, consumer psychology, and visual merchandising strategies into a highly polished, professional Executive Brief.

You will be given a complete JSON dataset containing rules, psychological profiles, and merchandising plans.
Construct an executive-ready report in clean Markdown.

The report must contain:
1. Executive Summary: A high-level overview of the basket analysis findings.
2. Strategic Action Plan: Break down the top product associations, explaining the "Why" (psychology) and the "How" (merchandising) side-by-side.
3. Projected Business Impact: A brief estimation of how these changes will impact Average Order Value (AOV) and customer retention.

Use professional, encouraging, and authoritative consulting language.
Do NOT write any conversational intro or outro text. Output ONLY the raw Markdown block.
"""

# ==========================================
# OLLAMA EXECUTIVE REPORTER RUNNER
# ==========================================

async def run_executive_reporter(combined_data: dict) -> str:
    """Invokes the Executive Coordinator to generate a beautifully structured Markdown report."""
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": REPORTER_SYSTEM_PROMPT},
            {"role": "user", "content": f"Generate an Executive Brief from this combined intelligence:\n{json.dumps(combined_data)}"}
        ],
        "stream": False,
        # We do NOT force JSON format here because we want a beautiful, human-readable Markdown document!
    }
    
    async with httpx.AsyncClient(timeout=robust_timeout) as client:
        response = await client.post(OLLAMA_URL, json=payload)
        response_data = response.json()
        return response_data["message"]["content"]