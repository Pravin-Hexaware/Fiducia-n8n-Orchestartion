from datetime import datetime
import os
import json
import fitz
from pathlib import Path
from langchain_classic.tools import tool
import pandas as pd
from utils.llm_testing import get_azure_chat_openai
import asyncio  # If needed for testing

LLM = get_azure_chat_openai()

# =================================Old up================
@tool
def scan_mandate_folder_and_parse() -> dict:
    """Scan input_fund_mandate/ → Extract LATEST PDF → Return dict with name + full text."""
    folder = Path(__file__).parent.parent / "input_fund_mandate"

    pdfs = list(folder.glob("*.pdf"))
    print(f"🔍 Found PDFs: {[p.name for p in pdfs]}")

    if not pdfs:
        return {"error": f"No PDF in {folder.absolute()}", "pdfs": []}

    latest = max(pdfs, key=os.path.getmtime)
    doc = fitz.open(latest)
    text = "".join(page.get_text() for page in doc)
    doc.close()

    print(f"✅ Parsed {latest.name}: {len(text)} chars")
    return {
        "pdf_name": latest.name,
        "full_text": text,
        "char_count": len(text)
    }


@tool
def extract_dynamic_criteria(raw_text: str, capability_params: str) -> str:
    """Extract criteria using dynamic capability_params → subprocess_name as keys."""
    try:
        # Parse capability_params JSON
        params = json.loads(capability_params)
        print(f"🔍 Processing {len(params)} subprocesses")

        # Build dynamic template
        dynamic_template = {
            "mandate": {
                "fund_name": "[fund name - e.g. 'ABC Fund']",
                "fund_size": "[fund size - e.g. '500 million USD']"
            }
        }

        for subprocess_id, details in params.items():
            subprocess_name = details['subprocess_name']
            data_elements = details['data_elements']
            dynamic_template["mandate"][subprocess_name] = {
                field: "" for field in data_elements
            }

        template_json = json.dumps(dynamic_template, indent=2)

        # LLM extraction prompt
        prompt = f"""Extract fund mandate criteria from PDF into EXACT JSON template.
Fill ONLY fields in template. Empty string "" if not found.
Please return JSON ONLY, no raw answers. Also ensure you make the dynamic fields as per capability_params.

For the Country names, Try to understand and match 
Eg : "United States", "USA", "U.S." → "US"

CRITICAL: For FINANCIAL PARAMETERS, detect thresholds and convert to SYMBOLS:

EXAMPLES FROM PDF TEXT → JSON OUTPUT:
• "ARR must exceed $35M" → "ARR": "> $35M USD"
• "Revenue over 50 million" → "Revenue": "> $50M USD" 
• "Market cap above $1B" → "Market Cap": "> $1B USD"
• "Less than 10% burn rate" → "Burn Rate": "< 10%"
• "EBITDA margin of 25-30%" → "EBITDA Margin": "= 25-30%"
• "P/E ratio between 15-20x" → "P/E Ratio": "= 15-20x"
• "Minimum $100M AUM" → "AUM": ">= $100M USD"

RULES:
1. Use >, <, >=, <=, = symbols ALWAYS for numbers
2. Keep USD, %, B, M suffixes
3. "Must be", "exceed", "above", "over" → >
4. "Less than", "below", "under" → <
5. "Between X-Y", "range X-Y" → "= X-Y"
6. Exact match → =


PDF TEXT (search carefully):
{raw_text}

EXACT JSON TEMPLATE:
{template_json}"""

        result = LLM.invoke(prompt).content.strip()
        print(f"✅ Dynamic extraction: {len(result)} chars")
        return result

    except json.JSONDecodeError as e:
        return f'{{"error": "Invalid capability_params JSON: {str(e)}"}}'
    except Exception as e:
        return f'{{"error": "Extraction failed: {str(e)}"}}'


from database.repositories.companyRepository import CompanyRepository
from tortoise.transactions import in_transaction
from langchain_core.tools import Tool
from database.models import Company , FundMandate, Sourcing


async def _async_load_and_filter_companies(user_filters_json: str) -> str:
    try:
        input_data = json.loads(user_filters_json)
        mandate_id = input_data.get("mandate_id")

        # 🚀 VALIDATE REQUIRED fund_mandate_id
        if not mandate_id:
            return json.dumps({
                "error": "fund_mandate_id REQUIRED. Format: {\"fund_mandate_id\": 1, \"additionalProp1\": {...}}"
            }, separators=(',', ':'))

        # Verify FundMandate exists
        fund_mandate = await FundMandate.get_or_none(id=mandate_id)
        if not fund_mandate:
            return json.dumps({
                "error": f"FundMandate id={mandate_id} not found"
            }, separators=(',', ':'))

        # Use 'additionalProp1' as the filters payload. If missing, treat as empty dict (fetch all companies).
        filters = input_data.get("additionalProp1") or {}

        # Filter mapping
        filter_mapping = {
            "geography": "country", "country": "country",
            "sector": "sector", "industry": "industry"
        }

        filter_conditions = {}
        for user_key, user_value in filters.items():
            actual_col = filter_mapping.get(user_key.lower())
            if actual_col:
                filter_conditions[f"{actual_col}__icontains"] = str(user_value).strip()

        async with in_transaction():
            total_companies = await Company.filter(deleted_at__isnull=True).count()

            if total_companies == 0:
                return json.dumps({
                    "total_companies": 0,
                    "qualified": [],
                    "message": "No companies in DB"
                }, default=str)

            # Choose query: if filter_conditions empty, fetch all companies (limit 100)
            if filter_conditions:
                filtered_query = Company.filter(deleted_at__isnull=True, **filter_conditions).limit(50)
            else:
                filtered_query = Company.filter(deleted_at__isnull=True).limit(100)

            filtered_companies = await filtered_query
            matched_count = len(filtered_companies)

            # � Save EACH company to Sourcing table (skip or update existing to avoid UNIQUE constraint)
            saved_sourcing_ids = []
            qualified = []
            for c in filtered_companies:
                company_data = {
                    **getattr(c, 'attributes', {}),
                    "Risks": c.risks if c.risks else {}
                }

                # Always create new - allows same company with different mandates
                sourcing = await Sourcing.create(
                    company_id=c.id,
                    company_data=company_data,
                    fund_mandate=fund_mandate,
                    selected_parameters=filters
                )
                saved_sourcing_ids.append(sourcing.company_id)

                # Build qualified entry with both id and company_id and a canonical Company name
                qualified.append({
                    "id": c.id,
                    "company_id": c.id,
                    "Company": getattr(c, 'company_name', None) or getattr(c, 'Company ', None) or '',
                    **getattr(c, 'attributes', {})
                })

        result = {
            "total_companies": total_companies,
            "qualified": qualified,
            "filters_applied": filters,
            "matched_count": matched_count,
            "sourcing_saved_ids": saved_sourcing_ids,  # [45,46,47]
            "mandate_id": mandate_id,
            "source": "database",
            "query_executed_at": datetime.utcnow().isoformat()
        }
        return json.dumps(result, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)}, separators=(',', ':'))


# ✅ SYNC WRAPPER for ToolNode (LangGraph v1.0.7)
def sync_load_and_filter_companies(user_filters_json: str) -> str:
    """Sync wrapper - LangGraph ToolNode calls this."""
    return asyncio.run(_async_load_and_filter_companies(user_filters_json))

# ✅ EXPORT THE TOOL (exact name your agent expects)
load_and_filter_companies = Tool(
    name="load_and_filter_companies",
    description="""Load companies from DATABASE → Filter by user filters → JSON with IDs (ACCURATE).
    Expects valid JSON: {"geography": "U.S.", "sector": "technology", "industry": "Software"} or {"additionalProp1": {...}}.
    Returns: total_companies, qualified list with IDs, filter details.""",
    func=sync_load_and_filter_companies,
)