import json
import io
import re
import queue
import asyncio
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.tools import tool
from langchain_classic.agents import AgentExecutor, create_tool_calling_agent
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, PageTemplate, Frame
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

load_dotenv()

# ============================================================================
# AZURE KEYVAULT SETUP
# ============================================================================

KEYVAULT_URI = "https://fstodevazureopenai.vault.azure.net/"
credential = DefaultAzureCredential()
kvclient = SecretClient(vault_url=KEYVAULT_URI, credential=credential)

secrets_map = {}
secret_names = ["llm-base-endpoint", "llm-mini", "llm-mini-version", "llm-api-key"]
for secret_name in secret_names:
    try:
        secret = kvclient.get_secret(secret_name)
        secrets_map[secret_name] = secret.value
    except Exception as e:
        print(f"Error retrieving secret '{secret_name}': {e}")
        raise

AZURE_OPENAI_ENDPOINT = secrets_map.get("llm-base-endpoint")
DEPLOYMENT_NAME = secrets_map.get("llm-mini")
OPENAI_API_VERSION = secrets_map.get("llm-mini-version")
GPT5_API_KEY = secrets_map.get("llm-api-key")


# ============================================================================
# EVENT STREAMING CALLBACK
# ============================================================================

class ReportEventCallback(BaseCallbackHandler):
    """Emits meaningful agent thinking and analysis events"""

    def __init__(self, event_queue=None):
        self.event_queue = event_queue
        self.buffer = ""
        self.token_count = 0
        self.sentence_endings = {'.', '!', '?'}

    def on_llm_new_token(self, token: str, **kwargs) -> None:
        """Buffers tokens and emits meaningful complete thoughts"""
        self.buffer += token
        self.token_count += 1

        has_sentence_ending = any(ending in self.buffer for ending in self.sentence_endings)

        if has_sentence_ending and self.token_count >= 30:
            content = self.buffer.strip()
            if content and len(content) > 20:
                if self.event_queue:
                    self.event_queue.put({
                        "type": "agent_thinking",
                        "content": content,
                        "timestamp": datetime.now().isoformat()
                    })
            self.buffer = ""
            self.token_count = 0

    def on_llm_end(self, response, **kwargs) -> None:
        """Flushes remaining content and captures token usage"""
        content = self.buffer.strip()
        if content and len(content) > 20:
            if self.event_queue:
                self.event_queue.put({
                    "type": "agent_thinking",
                    "content": content,
                    "timestamp": datetime.now().isoformat()
                })
        self.buffer = ""
        self.token_count = 0

        # Capture token usage from response
        if hasattr(response, 'usage_metadata'):
            usage = response.usage_metadata
            if usage:
                print(f"[CALLBACK TOKEN] Captured from usage_metadata: {usage}")
                accumulate_tokens(usage)

    def on_agent_action(self, action, **kwargs):
        """Capture agent's tool selection"""
        if self.event_queue:
            self.event_queue.put({
                "type": "agent_thinking",
                "content": f"Executing: {action.tool}",
                "timestamp": datetime.now().isoformat()
            })

    def on_tool_start(self, serialized: dict, input_str: str, **kwargs):
        """Tool is about to execute"""
        tool_name = serialized.get("name", "unknown")
        if self.event_queue:
            self.event_queue.put({
                "type": "tool_invocation",
                "tool": tool_name,
                "message": f"Processing {tool_name}...",
                "timestamp": datetime.now().isoformat()
            })


def get_azure_llm_report(event_queue=None):
    """Initialize Azure OpenAI for report generation with streaming"""
    try:
        return AzureChatOpenAI(
            azure_deployment=DEPLOYMENT_NAME,
            openai_api_version=OPENAI_API_VERSION,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=GPT5_API_KEY,
            temperature=1,
            streaming=True,
            callbacks=[ReportEventCallback(event_queue=event_queue)]
        )
    except Exception as e:
        print(f"Error initializing Azure LLM: {str(e)}")
        raise e


def get_azure_llm_report_for_tokens():
    """Initialize Azure OpenAI for report generation without streaming for reliable token capture"""
    try:
        return AzureChatOpenAI(
            azure_deployment=DEPLOYMENT_NAME,
            openai_api_version=OPENAI_API_VERSION,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=GPT5_API_KEY,
            temperature=1,
            streaming=False
        )
    except Exception as e:
        print(f"Error initializing Azure LLM (no streaming): {str(e)}")
        raise e


# ============================================================================
# GLOBAL STATE FOR REPORT GENERATION WORKFLOW
# ============================================================================

event_queue_global = None
tool_output_capture = {"last_json": None}
token_usage = {"prompt_tokens": 0, "completion_tokens": 0}


def set_event_queue_global(queue):
    """Sets the global event queue for real-time streaming"""
    global event_queue_global
    event_queue_global = queue


def reset_token_usage():
    """Resets token usage counters at the start of analysis"""
    global token_usage
    token_usage = {"prompt_tokens": 0, "completion_tokens": 0}


def accumulate_tokens(usage_dict: Dict[str, int]):
    """Accumulates token usage across all LLM calls"""
    global token_usage
    if usage_dict:
        token_usage["prompt_tokens"] += usage_dict.get("prompt_tokens", 0)
        token_usage["completion_tokens"] += usage_dict.get("completion_tokens", 0)
        print(
            f"[TOKEN] Accumulated - Prompt: {token_usage['prompt_tokens']}, Completion: {token_usage['completion_tokens']}")


def set_event_queue_global(queue):
    """Sets the global event queue for real-time streaming"""
    global event_queue_global
    event_queue_global = queue


# ============================================================================
# REPORT DATA STRUCTURES
# ============================================================================

class RiskParameter(BaseModel):
    """Represents a failed or notable risk parameter"""
    name: str = Field(description="Name of the parameter")
    status: str = Field(description="SAFE or UNSAFE")
    reason: str = Field(description="Detailed explanation")


class CompanyRiskSummary(BaseModel):
    """Summary of company risk assessment"""
    company_name: str = Field(description="Company name")
    overall_status: str = Field(description="SAFE or UNSAFE")
    safe_parameters: int = Field(description="Number of parameters passed")
    unsafe_parameters: int = Field(description="Number of parameters failed")
    failed_parameters: List[RiskParameter] = Field(default_factory=list)


class RiskReportData(BaseModel):
    """Complete risk assessment report data"""
    generated_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    total_companies: int = Field(description="Total companies analyzed")
    safe_companies: int = Field(description="Companies with SAFE status")
    unsafe_companies: int = Field(description="Companies with UNSAFE status")
    success_rate: float = Field(description="Percentage of safe companies")
    company_summaries: List[CompanyRiskSummary] = Field(default_factory=list)
    executive_summary: str = Field(description="Verbose executive summary by LLM")
    key_findings: List[str] = Field(description="Key findings from analysis")
    critical_risks: List[str] = Field(description="Critical risks requiring attention")


# ============================================================================
# SHARED DATA STORAGE FOR AGENT WORKFLOW
# ============================================================================

_agent_workflow_state = {
    "mandate_id": None,
    "mandate_details": None,
    "sourced_companies": [],
    "screened_companies": [],
    "risk_analysis": [],
    "risk_results": [],
    "report_data": None,
    "pdf_bytes": None,
    "output_path": None
}


def reset_workflow_state():
    """Reset the workflow state for a new report generation"""
    global _agent_workflow_state
    _agent_workflow_state = {
        "mandate_id": None,
        "mandate_details": None,
        "sourced_companies": [],
        "screened_companies": [],
        "risk_analysis": [],
        "risk_results": [],
        "report_data": None,
        "pdf_bytes": None,
        "output_path": None
    }


def set_workflow_mandate_id(mandate_id: int):
    """Set the mandate ID for the workflow"""
    _agent_workflow_state["mandate_id"] = mandate_id


print("Authenticating with Azure KeyVault...")
llm = get_azure_llm_report()
print("Azure OpenAI LLM Initialized for Report Generation")


# ============================================================================
# AGENT TOOLS - DATA FETCHING
# ============================================================================

@tool
def fetch_mandate_data(mandate_id: int) -> str:
    """

    Fetches all data from database for a given mandate.

    This tool retrieves:
    1. Mandate details (fund name, strategy, vintage year, etc.)
    2. Sourced companies (all companies identified during sourcing phase)
    3. Screened companies (screening decisions and reasons)
    4. Risk analysis results (detailed risk parameter assessments)

    The fetched data is stored in the workflow state for use by subsequent tools.

    Args:
        mandate_id: The ID of the fund mandate to fetch data for

    Returns:
        JSON string containing a summary of fetched data (record counts, mandate name)
    """
    try:
        # Run async database fetch
        db_data = asyncio.run(fetch_report_data_from_database(mandate_id, event_queue_global))

        # Store in workflow state
        _agent_workflow_state["mandate_id"] = mandate_id
        _agent_workflow_state["mandate_details"] = db_data.get('mandate_details')
        _agent_workflow_state["sourced_companies"] = db_data.get('sourced_companies', [])
        _agent_workflow_state["screened_companies"] = db_data.get('screened_companies', [])
        _agent_workflow_state["risk_analysis"] = db_data.get('risk_analysis', [])

        # Convert risk_analysis records from database into report format
        risk_analysis = db_data.get('risk_analysis', [])
        sourced_companies = db_data.get('sourced_companies', [])
        risk_results = []

        for risk_rec in risk_analysis:
            # Get company name from sourced_companies using company_id
            company_name = 'Unknown'
            if hasattr(risk_rec, 'company_id'):
                for company in sourced_companies:
                    if hasattr(company, 'id') and company.id == risk_rec.company_id:
                        company_name = company.company_name if hasattr(company, 'company_name') else 'Unknown'
                        break

            risk_result = {
                'company_name': company_name,
                'overall_result': getattr(risk_rec, 'overall_result', 'UNKNOWN'),
                'parameter_analysis': getattr(risk_rec, 'parameter_analysis', {}) or {},
                'overall_assessment': getattr(risk_rec, 'overall_assessment', {}) or {}
            }
            risk_results.append(risk_result)

        _agent_workflow_state["risk_results"] = risk_results

        # Log and emit event
        mandate_name = db_data.get('mandate_details', {}).get('legal_name', 'Unknown') if db_data.get(
            'mandate_details') else 'Unknown'
        log_msg = (
            f"✓ Fetched mandate data: {mandate_name} | "
            f"Sourced: {len(sourced_companies)} | "
            f"Screened: {len(db_data.get('screened_companies', []))} | "
            f"Risk analyzed: {len(risk_results)}"
        )
        print(f"[TOOL: fetch_mandate_data] {log_msg}")
        if event_queue_global:
            event_queue_global.put({
                'type': 'tool_result',
                'tool': 'fetch_mandate_data',
                'message': log_msg,
                'timestamp': datetime.now().isoformat()
            })

        return json.dumps({
            'status': 'success',
            'mandate_id': mandate_id,
            'mandate_name': mandate_name,
            'sourced_companies_count': len(sourced_companies),
            'screened_companies_count': len(db_data.get('screened_companies', [])),
            'risk_analyzed_count': len(risk_results)
        })

    except Exception as e:
        error_msg = f"Error fetching mandate data: {str(e)}"
        print(f"[TOOL: fetch_mandate_data] ERROR: {error_msg}")
        if event_queue_global:
            event_queue_global.put({
                'type': 'tool_error',
                'tool': 'fetch_mandate_data',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            })
        return json.dumps({'status': 'error', 'error': error_msg})


# ============================================================================
# AGENT TOOL - RISK ANALYSIS & REPORT GENERATION
# ============================================================================

@tool
def analyze_and_generate_report() -> str:
    """
    Analyzes all fetched data and generates a comprehensive risk assessment report.

    This tool uses the previously fetched mandate data and:
    1. Structures risk assessment data from database
    2. Generates LLM-based executive summary analyzing the full investment process
    3. Extracts key findings and critical risks
    4. Returns complete structured report data

    The report data is stored in the workflow state for use by subsequent tools.

    Args:
        (No arguments - uses workflow state data from fetch_mandate_data)

    Returns:
        JSON string containing the generated report data with executive summary and findings
    """
    try:
        risk_results = _agent_workflow_state.get("risk_results", [])
        screened_companies = _agent_workflow_state.get("screened_companies", [])
        sourced_companies = _agent_workflow_state.get("sourced_companies", [])

        if not risk_results:
            raise ValueError("No risk results found in workflow state. Run fetch_mandate_data first.")

        # ===== STEP 1: Parse and structure raw data =====
        company_summaries = []
        all_unsafe_params = {}
        total_safe = 0
        total_unsafe = 0

        for result in risk_results:
            company_name = result.get("company_name", "Unknown")
            overall_status = result.get("overall_result", "UNKNOWN")

            if overall_status == "SAFE":
                total_safe += 1
            else:
                total_unsafe += 1

            param_analysis = result.get("parameter_analysis", {})
            failed_params = []
            safe_count = 0
            unsafe_count = 0

            for param_name, param_data in param_analysis.items():
                if isinstance(param_data, dict):
                    status = param_data.get("status", "UNKNOWN")
                    reason = param_data.get("reason", "No details")

                    if status == "SAFE":
                        safe_count += 1
                    else:
                        unsafe_count += 1
                        failed_params.append({
                            "name": param_name,
                            "status": status,
                            "reason": reason
                        })
                        all_unsafe_params[param_name] = all_unsafe_params.get(param_name, 0) + 1

            company_summaries.append({
                "company_name": company_name,
                "overall_status": overall_status,
                "safe_parameters": safe_count,
                "unsafe_parameters": unsafe_count,
                "failed_parameters": failed_params
            })

        total_companies = len(risk_results)
        success_rate = (total_safe / total_companies * 100) if total_companies > 0 else 0

        # ===== STEP 2: Generate executive summary using LLM =====
        companies_text = "\n".join([
            f"{s['company_name']}: {s['overall_status']} "
            f"({s['safe_parameters']} passed, {s['unsafe_parameters']} failed)"
            for s in company_summaries
        ])

        critical_params_text = "\n".join([
            f"{param}: {count} company(ies) affected"
            for param, count in sorted(all_unsafe_params.items(), key=lambda x: x[1], reverse=True)[:5]
        ]) if all_unsafe_params else "No critical parameters identified"

        summary_prompt = ChatPromptTemplate.from_template("""
You are a Professional Investment Risk Analyst. Analyze the following risk assessment data and provide a comprehensive executive summary.

ASSESSMENT STATISTICS:
- Total Companies Analyzed: {total_companies}
- Companies Passed All Checks: {safe_companies}
- Companies Failed Checks: {unsafe_companies}
- Success Rate: {success_rate}%

COMPANY SUMMARIES:
{companies_summary}

CRITICAL PARAMETERS FAILURES:
{critical_params}

Provide a detailed executive summary (4-5 sentences) analyzing the overall investment risk landscape, key observations about company compliance patterns, and assessment of portfolio risk concentration.
Write in a professional, formal tone suitable for executive stakeholders.
""")

        llm_instance = get_azure_llm_report(event_queue=event_queue_global)

        summary_response = (summary_prompt | llm_instance).invoke({
            "total_companies": total_companies,
            "safe_companies": total_safe,
            "unsafe_companies": total_unsafe,
            "success_rate": round(success_rate, 2),
            "companies_summary": companies_text,
            "critical_params": critical_params_text
        })

        executive_summary = summary_response.content if hasattr(summary_response, 'content') else str(summary_response)

        # ===== STEP 3: Build comprehensive context for key findings =====
        sourcing_context = ""
        if sourced_companies:
            sourcing_context = "SOURCED COMPANIES:\n"
            for i, company in enumerate(sourced_companies, 1):
                company_name = company.company_name if hasattr(company, 'company_name') else 'Unknown'
                sector = company.sector if hasattr(company, 'sector') else 'N/A'
                industry = company.industry if hasattr(company, 'industry') else 'N/A'
                sourcing_context += f"  {i}. {company_name} - {sector} / {industry}\n"

        screening_context = ""
        if screened_companies:
            screening_context = "\nSCREENED COMPANIES:\n"
            for i, screening in enumerate(screened_companies, 1):
                company_id = getattr(screening, 'company_id', None)
                company_name = f"Company {company_id}" if company_id else "Unknown"
                status = getattr(screening, 'status', 'UNKNOWN')
                reason = getattr(screening, 'reason', 'No details')[:100]
                screening_context += f"  {i}. {company_name} - {status}: {reason}\n"

        # ===== STEP 4: Extract key findings using LLM =====
        findings_prompt = ChatPromptTemplate.from_template("""
You are a Professional Investment Risk Analyst. Analyze the entire investment process (sourcing → screening → risk assessment) and extract key findings.

STATISTICS:
- Total Companies Analyzed: {total_companies}
- Companies Passed Risk Assessment: {safe_companies}
- Companies Failed Risk Assessment: {unsafe_companies}
- Success Rate: {success_rate}%

{sourcing_context}

{screening_context}

RISK ASSESSMENT RESULTS:
{companies_summary}

CRITICAL PARAMETERS:
{critical_params}

Extract 5-7 key findings as actionable investment insights. Return ONLY a JSON array:
["Finding 1", "Finding 2", "Finding 3", ...]
""")

        findings_response = (findings_prompt | llm_instance).invoke({
            "total_companies": total_companies,
            "safe_companies": total_safe,
            "unsafe_companies": total_unsafe,
            "success_rate": success_rate,
            "sourcing_context": sourcing_context,
            "screening_context": screening_context,
            "companies_summary": companies_text,
            "critical_params": critical_params_text
        })

        findings_text = findings_response.content if hasattr(findings_response, 'content') else str(findings_response)

        key_findings = []
        try:
            json_match = re.search(r'\[.*\]', findings_text, re.DOTALL)
            if json_match:
                key_findings = json.loads(json_match.group())
                key_findings = [str(f) for f in key_findings if f]
        except:
            key_findings = [
                "Comprehensive sourcing strategy identified key market segments",
                f"Screening process achieved {success_rate:.1f}% compliance rate",
                f"Risk assessment identified {total_unsafe} companies requiring attention",
                "Critical parameters flagged for portfolio monitoring",
                "Structured investment decision framework applied successfully"
            ]

        # ===== STEP 5: Identify critical risks =====
        critical_risks = []
        for param, count in sorted(all_unsafe_params.items(), key=lambda x: x[1], reverse=True)[:3]:
            critical_risks.append(f"{param} - Affecting {count} company(ies)")

        # ===== STEP 6: Assemble final report =====
        report_data = {
            "generated_at": datetime.now().isoformat(),
            "total_companies": total_companies,
            "safe_companies": total_safe,
            "unsafe_companies": total_unsafe,
            "success_rate": round(success_rate, 2),
            "company_summaries": company_summaries,
            "executive_summary": executive_summary,
            "key_findings": key_findings[:5],
            "critical_risks": critical_risks
        }

        # Store in workflow state
        _agent_workflow_state["report_data"] = report_data

        log_msg = (
            f"✓ Generated risk assessment report | "
            f"Total companies: {report_data.get('total_companies')} | "
            f"Safe: {report_data.get('safe_companies')} | "
            f"Unsafe: {report_data.get('unsafe_companies')} | "
            f"Success rate: {report_data.get('success_rate', 0):.1f}%"
        )
        print(f"[TOOL: analyze_and_generate_report] {log_msg}")
        if event_queue_global:
            event_queue_global.put({
                'type': 'tool_result',
                'tool': 'analyze_and_generate_report',
                'message': log_msg,
                'timestamp': datetime.now().isoformat()
            })

        return json.dumps(report_data, indent=2)

    except Exception as e:
        error_msg = f"Error in report analysis: {str(e)}"
        print(f"[TOOL: analyze_and_generate_report] ERROR: {error_msg}")
        traceback.print_exc()
        if event_queue_global:
            event_queue_global.put({
                'type': 'tool_error',
                'tool': 'analyze_and_generate_report',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            })
        return json.dumps({'error': error_msg})


# ============================================================================
# AGENT TOOL - PDF GENERATION
# ============================================================================

@tool
def generate_pdf_report(output_path: str = None) -> str:
    """
    Generates a professional PDF report from the analyzed report data.

    This tool uses the previously generated report data and creates a polished
    PDF document with:
    1. Mandate details and fund information
    2. Executive summary and key findings
    3. Sourced and screened companies
    4. Risk assessment results with detailed parameters
    5. Professional formatting with borders and page numbers

    The PDF is stored in the workflow state and optionally saved to disk.

    Args:
        output_path: Optional file path to save the PDF. If not provided, PDF is only generated in memory.

    Returns:
        JSON string containing PDF generation status, file path, and file size
    """
    try:
        report_data = _agent_workflow_state.get("report_data")
        mandate_details = _agent_workflow_state.get("mandate_details")
        sourced_companies = _agent_workflow_state.get("sourced_companies")
        screened_companies = _agent_workflow_state.get("screened_companies")
        risk_analysis = _agent_workflow_state.get("risk_analysis")

        if not report_data:
            raise ValueError("No report data found in workflow state. Run analyze_and_generate_report first.")

        # Generate PDF
        pdf_bytes = _build_pdf_from_report(
            report_data,
            mandate_details=mandate_details,
            sourced_companies=sourced_companies,
            screened_companies=screened_companies,
            risk_analysis=risk_analysis
        )

        _agent_workflow_state["pdf_bytes"] = pdf_bytes

        file_path = None
        if output_path:
            output_path_obj = Path(output_path)
            output_path_obj.parent.mkdir(parents=True, exist_ok=True)

            # Add timestamp to filename if it's generic
            if output_path_obj.stem == 'report':
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path_obj = output_path_obj.parent / f"report_{timestamp}.pdf"

            with open(output_path_obj, 'wb') as f:
                f.write(pdf_bytes)
            file_path = str(output_path_obj.absolute())
            _agent_workflow_state["output_path"] = file_path

        log_msg = f"✓ Generated PDF report | Size: {len(pdf_bytes)} bytes"
        if file_path:
            log_msg += f" | Saved to: {file_path}"
        print(f"[TOOL: generate_pdf_report] {log_msg}")
        if event_queue_global:
            event_queue_global.put({
                'type': 'tool_result',
                'tool': 'generate_pdf_report',
                'message': log_msg,
                'timestamp': datetime.now().isoformat()
            })

        return json.dumps({
            'status': 'success',
            'pdf_size_bytes': len(pdf_bytes),
            'file_path': file_path,
            'in_memory': True,
            'success_rate': report_data.get('success_rate', 0)
        })

    except Exception as e:
        error_msg = f"Error generating PDF: {str(e)}"
        print(f"[TOOL: generate_pdf_report] ERROR: {error_msg}")
        if event_queue_global:
            event_queue_global.put({
                'type': 'tool_error',
                'tool': 'generate_pdf_report',
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            })
        return json.dumps({'status': 'error', 'error': error_msg})


# ============================================================================
# AGENT TOOL - REPORT GENERATION (legacy - kept for backward compatibility)
# ============================================================================


# ============================================================================
# REPORT GENERATION AGENT
# ============================================================================

REPORT_AGENT_SYSTEM_PROMPT = """You are a Professional Investment Report Generation Agent.

Your role is to orchestrate a comprehensive multi-stage report generation workflow for investment fund analysis.
You fetch all agent outputs (sourcing, screening, risk analysis) directly from the database and create a professional report.

AVAILABLE TOOLS - Use them in sequence:

1. fetch_mandate_data(mandate_id)
   - Fetches ALL data from database in one operation:
     * Mandate details (fund name, strategy, vintage year, etc.)
     * Sourced companies (companies identified by sourcing agent)
     * Screened companies (screening agent decisions and reasons)
     * Risk analysis results (risk assessment agent outputs)
   - Use this FIRST to load all database records
   - Returns: Summary of fetched data with record counts

2. analyze_and_generate_report()
   - Analyzes all fetched data comprehensively:
     * Structures risk assessment data from database
     * Uses LLM to generate executive summary analyzing the entire investment process
     * Extracts key findings from sourcing → screening → risk assessment pipeline
     * Identifies critical risks requiring attention
     * Builds complete structured report data
   - Use this SECOND after fetching data
   - No arguments - uses data from workflow state
   - Returns: Structured report JSON with LLM analysis

3. generate_pdf_report(output_path)
   - Generates professional PDF document:
     * Mandate details and fund information
     * Executive summary and key findings
     * Sourced companies table
     * Screened companies with decisions
     * Risk assessment results with detailed parameters
     * Professional formatting with borders and page numbers
   - Use this THIRD after analysis
   - Arguments: Optional output_path to save PDF file
   - Returns: PDF generation status and file path

WORKFLOW SEQUENCE:
1. User provides mandate_id
2. Use fetch_mandate_data to load ALL data from database (sourcing, screening, risk analysis outputs)
3. Use analyze_and_generate_report to synthesize data and generate LLM-based analysis
4. Use generate_pdf_report to create final professional PDF
5. Report is complete with full investment process analysis

KEY PRINCIPLE: All data comes from the database. No manual data formatting needed.
Always follow the three-step sequence strictly. Each tool depends on the previous one."""


def create_report_generation_agent(event_queue=None):
    """
    Creates and returns a configured report generation agent with new tool-based workflow.

    This agent orchestrates the complete report generation pipeline:
    1. Fetches mandate data from database
    2. Analyzes risk data and generates report
    3. Creates professional PDF

    Args:
        event_queue: Optional queue for streaming progress events

    Returns:
        AgentExecutor configured for multi-step report generation workflow
    """
    set_event_queue_global(event_queue)

    llm_instance = get_azure_llm_report(event_queue=event_queue)

    # Define tools for the new agent workflow
    tools = [
        fetch_mandate_data,
        analyze_and_generate_report,
        generate_pdf_report
    ]

    # Create the agent
    agent_prompt = ChatPromptTemplate.from_messages([
        ("system", REPORT_AGENT_SYSTEM_PROMPT),
        ("user", "{input}"),
        ("assistant", "{agent_scratchpad}")
    ])

    agent = create_tool_calling_agent(llm_instance, tools, agent_prompt)

    # Create callbacks for agent executor
    agent_callbacks = [ReportEventCallback(event_queue=event_queue)]

    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        handle_parsing_errors=True,
        max_iterations=15,  # More iterations for multi-step workflow
        callbacks=agent_callbacks
    )

    return agent_executor


# ============================================================================
# MAIN ORCHESTRATION - Agent-Based Workflow
# ============================================================================

def create_report_with_agent(
        mandate_id: int,
        output_path: Optional[str] = None,
        event_queue: Optional[Any] = None
) -> tuple:
    """
    Creates a comprehensive report using the agent-based workflow.

    This is the PRIMARY recommended entry point. It orchestrates the complete
    report generation pipeline through the agent:
    1. Fetches all data via fetch_mandate_data tool
    2. Analyzes risk via analyze_and_generate_report tool
    3. Generates PDF via generate_pdf_report tool

    Args:
        mandate_id: ID of the fund mandate to generate report for
        output_path: Optional path to save PDF file
        event_queue: Optional queue for streaming progress events

    Returns:
        Tuple of (file_path, pdf_bytes, report_json_string)
    """
    try:
        # Reset workflow state and token usage
        reset_workflow_state()
        reset_token_usage()
        set_event_queue_global(event_queue)

        # Emit session start event
        if event_queue:
            event_queue.put({
                'type': 'report_session_start',
                'message': f'Starting agent-based report generation for mandate {mandate_id}',
                'timestamp': datetime.now().isoformat()
            })

        # Create and run the agent
        agent = create_report_generation_agent(event_queue=event_queue)

        # Build the user request
        user_request = f"""
Generate a comprehensive risk assessment report for mandate ID {mandate_id}.

Follow the standard workflow:
1. First, fetch all mandate data using fetch_mandate_data
2. Then, analyze the risk data and generate report using analyze_and_generate_report
3. Finally, generate the PDF report using generate_pdf_report with output_path='{output_path}'

Please complete all three steps in order.
"""

        # Run the agent
        result = agent.invoke({'input': user_request})

        # Extract final results from workflow state
        file_path = _agent_workflow_state.get("output_path")
        pdf_bytes = _agent_workflow_state.get("pdf_bytes", b'')
        report_data = _agent_workflow_state.get("report_data", {})

        # Print token summary
        print(
            f"[TOKEN SUMMARY] Total tokens used - Prompt: {token_usage['prompt_tokens']}, Completion: {token_usage['completion_tokens']}")

        # Emit session complete event with token usage
        if event_queue:
            event_queue.put({
                'type': 'report_complete',
                'message': 'Report generation complete',
                'timestamp': datetime.now().isoformat(),
                'file_path': file_path,
                'pdf_size_bytes': len(pdf_bytes),
                'token_usage': token_usage,
                'report_summary': {
                    'total_companies': report_data.get('total_companies'),
                    'safe_companies': report_data.get('safe_companies'),
                    'unsafe_companies': report_data.get('unsafe_companies'),
                    'success_rate': report_data.get('success_rate')
                }
            })

        return (file_path, pdf_bytes, json.dumps(report_data, indent=2))

    except Exception as e:
        error_msg = f"Agent-based report generation failed: {str(e)}"
        print(f"[AGENT WORKFLOW ERROR] {error_msg}")
        traceback.print_exc()
        if event_queue:
            event_queue.put({
                'type': 'error',
                'message': error_msg,
                'timestamp': datetime.now().isoformat()
            })
        raise


# ============================================================================
# LEGACY FUNCTIONS - Maintained for backward compatibility
# ============================================================================
# PROFESSIONAL PDF REPORT GENERATION
# ============================================================================

def _build_pdf_from_report(
        report_dict: Dict[str, Any],
        mandate_details: Optional[Dict[str, Any]] = None,
        sourced_companies: Optional[List[Any]] = None,
        screened_companies: Optional[List[Any]] = None,
        risk_analysis: Optional[List[Any]] = None
) -> bytes:
    """Generate a professional PDF report from structured report data with mandate details and page numbers"""
    pdf_buffer = io.BytesIO()

    # ===== UTILITY FUNCTION: Clean all bullet characters from text =====
    def clean_text(text):
        """Remove all special characters and noise that might render as black squares"""
        if not isinstance(text, str):
            return text

        # STEP 1: Remove ALL Unicode bullet and special characters
        # This is a comprehensive list to prevent any black squares
        chars_to_remove = [
            # Standard bullets
            '•', '●', '◆', '◇', '○', '■', '□', '▪', '▫', '◉', '◎', '◌', '◍', '◎', '⊙', '⊚', '⊛',
            # Variations and unicode bullets
            '‣', '›', '⁌', '⁍', '⁎', '‹',
            # Checkmarks and X marks
            '✓', '✗', '✔', '✘', '✕', '✖', '✚', '✜', '✝', '✞', '✟', '✠', '✡', '✢', '✣', '✤', '✥',
            # Arrows and pointers
            '➤', '➢', '▸', '▹', '►', '▶', '▷', '▶', '→', '⇒', '↦', '⟹', '⟶', '←', '⇐', '↤', '⟸', '⟵',
            # Other symbols that might render as dots
            '∙', '∘', '⋅', '◦', '‧', '⋆', '★', '✦', '✧', '⁎', '⁕', '⁘', '⁙', '⁚', '⁛', '⁜',
            # Dashes and hyphens (but keep regular dash for readability)
            '–', '—', '−', '‐', '‑', '‒', '−', '⸺', '⸻',
            # Additional symbols that might appear
            '※', '‼', '⁇', '⁈', '⁉', '‾', '‿', '⁀', '⁁', '⁂', '⁃', '⁄', '⁅', '⁆', '⁇', '⁈', '⁉', '⁊', '⁋',
            # Box drawing characters
            '┌', '┐', '└', '┘', '├', '┤', '┬', '┴', '┼', '│', '─', '┕', '┗', '┑', '┒', '┓', '┏',
            # Mathematical operators that might render oddly
            '±', '∓', '×', '÷', '∗', '∞', '√', '∜', '∛', '∝', '∞', '≈', '≉', '≠', '≡', '≢',
        ]

        # Remove each problematic character
        for char in chars_to_remove:
            text = text.replace(char, '')

        # STEP 2: Remove leading bullet patterns at line start with any number of spaces
        # This regex removes: "• ", "* ", "- ", etc. at start of lines
        text = re.sub(r'^[\s]*[-\*•●◆◇○■□▪▫‣›⁌⁍⁎✓✗✔✘✕✖➤➢▸▹►∙∘⋅◦‧]+[\s]*', '', text, flags=re.MULTILINE)

        # STEP 3: Remove any remaining leading special punctuation at line start
        text = re.sub(r'^[\s]*[!@#$%^&*()+=\[\]{}|;:\'",<>/?\\~`]+[\s]*', '', text, flags=re.MULTILINE)

        # STEP 4: Remove multiple consecutive spaces and tabs
        text = re.sub(r'[\s]{2,}', ' ', text)

        # STEP 5: Remove any remaining standalone special characters or sequences
        # But preserve letters, numbers, common punctuation (., , : ; ! ? -), and spaces
        text = re.sub(r'[^\w\s\.\,\:\;\!\?\-\(\)\'\"]', '', text)

        # STEP 6: Clean up spacing around punctuation
        text = re.sub(r'\s+([.,:;!?])', r'\1', text)  # Remove space before punctuation
        text = re.sub(r'([.,:;!?])\s+', r'\1 ', text)  # Single space after punctuation

        # STEP 7: Final cleanup - remove leading/trailing whitespace
        return text.strip()

    # Create a custom canvas class for page borders and page numbers
    class BorderedCanvas:
        def __init__(self, *args, **kwargs):
            pass

        def __call__(self, canvas, doc):
            """Add professional borders and page numbers to each page"""
            canvas.saveState()

            # Draw professional border around page
            from reportlab.lib.colors import HexColor
            border_color = HexColor('#1a3a52')
            canvas.setStrokeColor(border_color)
            canvas.setLineWidth(2)

            # Draw rectangle border with some margin
            margin = 0.5 * inch
            canvas.rect(margin, margin, letter[0] - 2 * margin, letter[1] - 2 * margin, stroke=1, fill=0)

            # Add inner subtle border
            canvas.setLineWidth(0.5)
            inner_margin = 0.6 * inch
            canvas.rect(inner_margin, inner_margin, letter[0] - 2 * inner_margin, letter[1] - 2 * inner_margin,
                        stroke=1, fill=0)

            # Add page number at bottom right (positioned lower to avoid border collision)
            canvas.setFont("Helvetica", 9)
            page_num = canvas.getPageNumber()
            canvas.drawRightString(7.2 * inch, 0.3 * inch, f"Page {page_num}")

            canvas.restoreState()

    doc = SimpleDocTemplate(
        pdf_buffer,
        pagesize=letter,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch
    )

    styles = getSampleStyleSheet()

    # Define professional custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=26,
        textColor=colors.HexColor('#1a3a52'),
        spaceAfter=6,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )

    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#555555'),
        alignment=TA_CENTER,
        spaceAfter=6,
        fontName='Helvetica'
    )

    # Mandate details subtitle style - smaller than main title
    mandate_subtitle_style = ParagraphStyle(
        'MandateSubtitle',
        parent=styles['Normal'],
        fontSize=16,
        textColor=colors.HexColor('#1a3a52'),
        alignment=TA_CENTER,
        spaceAfter=8,
        spaceBefore=4,
        fontName='Helvetica-Bold'
    )

    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=13,
        textColor=colors.HexColor('#1a3a52'),
        spaceAfter=6,
        spaceBefore=8,
        fontName='Helvetica-Bold',
        borderColor=colors.HexColor('#1a3a52'),
        borderWidth=1,
        borderPadding=4
    )

    subheading_style = ParagraphStyle(
        'CustomSubHeading',
        parent=styles['Heading3'],
        fontSize=10,
        textColor=colors.HexColor('#2e5266'),
        spaceAfter=3,
        spaceBefore=6,
        fontName='Helvetica-Bold'
    )

    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['Normal'],
        fontSize=9,
        alignment=TA_JUSTIFY,
        spaceAfter=3,
        leading=11
    )

    story = []

    # ===== CREATE COMPANY ID TO COMPANY NAME MAPPING =====
    # Build a map of company_id -> company_name from sourced_companies list
    # Apply clean_text() to all database fields to remove Unicode noise
    company_map = {}
    if sourced_companies:
        for company in sourced_companies:
            if hasattr(company, 'id') and hasattr(company, 'company_name'):
                company_map[company.id] = {
                    'name': clean_text(company.company_name),
                    'sector': clean_text(getattr(company, 'sector', 'N/A')),
                    'industry': clean_text(getattr(company, 'industry', 'N/A')),
                    'country': clean_text(getattr(company, 'country', 'N/A'))
                }

    # ===== PAGE 1: TITLE & MANDATE DETAILS ONLY =====
    story.append(Paragraph("RISK ASSESSMENT REPORT", title_style))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph("MANDATE DETAILS", mandate_subtitle_style))

    # ===== MANDATE DETAILS TABLE (FULLY DYNAMIC & CENTERED) =====
    if mandate_details:
        # Dynamically build mandate details based on what's available
        details_rows = []

        # Add fields that exist
        if mandate_details.get('legal_name'):
            details_rows.append(["Fund Name", clean_text(mandate_details.get('legal_name'))])

        if mandate_details.get('strategy_type'):
            details_rows.append(["Strategy Type", clean_text(mandate_details.get('strategy_type'))])

        if mandate_details.get('vintage_year'):
            details_rows.append(["Vintage Year", str(mandate_details.get('vintage_year'))])

        if mandate_details.get('primary_analyst'):
            details_rows.append(["Primary Analyst", clean_text(mandate_details.get('primary_analyst'))])

        if mandate_details.get('processing_date'):
            details_rows.append(["Processing Date", clean_text(mandate_details.get('processing_date'))])

        if mandate_details.get('description') and mandate_details.get('description').strip():
            desc_para = Paragraph(clean_text(mandate_details.get('description')), body_style)
            details_rows.append(["Description", desc_para])

        # Calculate dynamic column width based on number of rows
        # Adjust vertical spacing based on content
        content_height = 0.2 * inch * len(details_rows)
        vertical_spacer = max(0.5 * inch, (7.5 * inch - content_height) / 2)

        story.append(Spacer(1, vertical_spacer))

        # Calculate dynamic column widths
        # Label column width based on longest label
        label_width = 1.8 * inch
        value_width = 3.2 * inch

        # Create dynamic table
        if details_rows:
            mandate_table = Table(details_rows, colWidths=[label_width, value_width])
            mandate_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (1, 0), colors.HexColor('#1a3a52')),
                ('TEXTCOLOR', (0, 0), (1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 0), (1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (1, 0), 8),
                ('TOPPADDING', (0, 0), (1, 0), 8),
                ('BACKGROUND', (0, 1), (1, -1), colors.HexColor('#f5f5f5')),
                ('GRID', (0, 0), (1, -1), 1.5, colors.HexColor('#1a3a52')),
                ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 1), (0, -1), 9),
                ('FONTSIZE', (1, 1), (1, -1), 9),
                ('VALIGN', (0, 1), (1, -1), 'TOP'),
                ('ROWBACKGROUNDS', (0, 1), (1, -1), [colors.white, colors.HexColor('#f9f9f9')]),
                ('TOPPADDING', (0, 1), (1, -1), 6),
                ('BOTTOMPADDING', (0, 1), (1, -1), 6),
                ('LEFTPADDING', (0, 0), (1, -1), 12),
                ('RIGHTPADDING', (0, 0), (1, -1), 12)
            ]))

            # Create a centered wrapper for the table
            from reportlab.platypus import Table as RLTable
            centered_wrapper = RLTable([[mandate_table]], colWidths=[5.0 * inch])
            centered_wrapper.setStyle(TableStyle([
                ('ALIGN', (0, 0), (0, 0), 'CENTER'),
                ('VALIGN', (0, 0), (0, 0), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (0, 0), 0),
                ('RIGHTPADDING', (0, 0), (0, 0), 0),
                ('TOPPADDING', (0, 0), (0, 0), 0),
                ('BOTTOMPADDING', (0, 0), (0, 0), 0)
            ]))

            story.append(centered_wrapper)

        # Page break after mandate table
        story.append(PageBreak())
    else:
        # If no mandate details, just page break
        story.append(Spacer(1, 0.5 * inch))
        story.append(PageBreak())

    # ===== PAGE 2+: ANALYSIS SECTIONS START HERE =====
    story.append(Paragraph("OVERVIEW & ASSESSMENT", heading_style))

    # Build comprehensive overview with all three contexts
    overview_text = """
This comprehensive risk assessment report covers the entire investment process across three critical stages:
"""
    story.append(Paragraph(clean_text(overview_text), body_style))
    story.append(Spacer(1, 0.04 * inch))

    # Add sourcing overview
    story.append(Paragraph("<b>1. SOURCING STAGE:</b>", subheading_style))
    if sourced_companies:
        sourcing_summary = f"Identified and evaluated {len(sourced_companies)} potential investment opportunities across multiple sectors and geographies."
        story.append(Paragraph(clean_text(sourcing_summary), body_style))
    story.append(Spacer(1, 0.03 * inch))

    # Add screening overview
    story.append(Paragraph("<b>2. SCREENING STAGE:</b>", subheading_style))
    if screened_companies:
        rejected_count = len(screened_companies)
        screening_summary = f"Screened {len(screened_companies)} companies against mandate criteria. {rejected_count} companies were evaluated based on strategic fit and preliminary risk metrics."
        story.append(Paragraph(clean_text(screening_summary), body_style))
    story.append(Spacer(1, 0.03 * inch))

    # Add risk assessment overview
    story.append(Paragraph("<b>3. RISK ASSESSMENT STAGE:</b>", subheading_style))
    total_cos = report_dict.get('total_companies', 0)
    safe_cos = report_dict.get('safe_companies', 0)
    unsafe_cos = report_dict.get('unsafe_companies', 0)
    success_rate = report_dict.get('success_rate', 0)
    risk_summary = f"Conducted detailed risk analysis on {total_cos} companies. {safe_cos} companies ({success_rate:.1f}%) met all risk parameters and passed assessment. {unsafe_cos} companies require attention due to parameter failures or risk mitigation needs."
    story.append(Paragraph(clean_text(risk_summary), body_style))
    story.append(Spacer(1, 0.08 * inch))

    # Add executive summary from LLM
    story.append(Paragraph("<b>EXECUTIVE SUMMARY:</b>", subheading_style))
    executive_summary = report_dict.get('executive_summary', 'No summary available')
    # Clean any bullet characters from executive summary - aggressive cleaning
    executive_summary = clean_text(executive_summary)
    story.append(Paragraph(clean_text(executive_summary), body_style))
    story.append(Spacer(1, 0.08 * inch))

    # ===== EXECUTIVE SUMMARY TABLE (IN THE MIDDLE) =====
    story.append(Paragraph("EXECUTIVE SUMMARY", heading_style))

    exec_data = [
        ['Metric', 'Value', 'Percentage'],
        ['Total Companies', str(report_dict.get('total_companies', 0)), '100%'],
        ['Companies - SAFE', str(report_dict.get('safe_companies', 0)),
         f"{report_dict.get('success_rate', 0):.1f}%"],
        ['Companies - UNSAFE', str(report_dict.get('unsafe_companies', 0)),
         f"{100 - report_dict.get('success_rate', 0):.1f}%"]
    ]

    exec_table = Table(exec_data, colWidths=[2.0 * inch, 1.5 * inch, 1.5 * inch])
    exec_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a3a52')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f0f0f0')),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#cccccc')),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9f9f9')]),
        ('TOPPADDING', (0, 1), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 5),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6)
    ]))
    story.append(exec_table)
    story.append(Spacer(1, 0.08 * inch))

    # ===== SOURCED COMPANIES SECTION =====
    if sourced_companies:
        story.append(Paragraph("SOURCED COMPANIES", heading_style))
        sourced_data = [['Company Name', 'Sector', 'Industry', 'Country']]
        for company in sourced_companies:
            # Use company_map that was built at the beginning
            company_id = company.id if hasattr(company, 'id') else None
            if company_id and company_id in company_map:
                company_name = company_map[company_id]['name']
                sector = company_map[company_id]['sector']
                industry = company_map[company_id]['industry']
                country = company_map[company_id]['country']
            else:
                # Fallback to direct attributes - also clean them
                company_name = clean_text(company.company_name if hasattr(company, 'company_name') else 'N/A')
                sector = clean_text(company.sector if hasattr(company, 'sector') else 'N/A')
                industry = clean_text(company.industry if hasattr(company, 'industry') else 'N/A')
                country = clean_text(company.country if hasattr(company, 'country') else 'N/A')

            sourced_data.append([
                company_name,
                sector,
                industry,
                country
            ])

        sourced_table = Table(sourced_data, colWidths=[2.0 * inch, 1.5 * inch, 1.5 * inch, 1.0 * inch])
        sourced_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a3a52')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f5')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9f9f9')]),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('WORDWRAP', (0, 0), (-1, -1), True)
        ]))
        story.append(sourced_table)
        story.append(Spacer(1, 0.08 * inch))

    # ===== SCREENED COMPANIES SECTION =====
    if screened_companies:
        story.append(Paragraph("SCREENED COMPANIES", heading_style))
        screened_data = [['Company Name', 'Status', 'Reason']]
        for screening in screened_companies:
            # Use company_id to lookup company name from company_map
            company_id = getattr(screening, 'company_id', None)
            if company_id and company_id in company_map:
                company_name = company_map[company_id]['name']
            elif company_id:
                company_name = f"Company {company_id}"
            else:
                company_name = 'N/A'

            status = clean_text(screening.status if hasattr(screening, 'status') and screening.status else 'N/A')
            reason = clean_text(
                screening.reason if hasattr(screening, 'reason') and screening.reason else 'No details')[:60]
            screened_data.append([company_name, status, reason])

        # Dynamic column widths based on available space (6.5 inches available after margins)
        # Company Name: 1.8 inches, Status: 1.0 inches, Reason: 3.7 inches
        screened_table = Table(screened_data, colWidths=[1.8 * inch, 1.0 * inch, 3.7 * inch])
        screened_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a3a52')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (0, -1), 'MIDDLE'),
            ('VALIGN', (1, 0), (1, -1), 'MIDDLE'),
            ('VALIGN', (2, 0), (2, -1), 'TOP'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 7.5),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f5')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9f9f9')]),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('LEFTPADDING', (0, 0), (0, -1), 6),
            ('LEFTPADDING', (1, 0), (1, -1), 6),
            ('LEFTPADDING', (2, 0), (2, -1), 8),
            ('RIGHTPADDING', (0, 0), (0, -1), 6),
            ('RIGHTPADDING', (1, 0), (1, -1), 6),
            ('RIGHTPADDING', (2, 0), (2, -1), 8),
            ('WORDWRAP', (2, 0), (2, -1), True)
        ]))
        story.append(screened_table)
        story.append(Spacer(1, 0.08 * inch))
    # ===== CRITICAL RISKS (moved before Risk Assessment Results) =====
    critical_risks = report_dict.get('critical_risks', [])
    if critical_risks:
        story.append(Paragraph("CRITICAL RISK PARAMETERS", heading_style))
        story.append(Spacer(1, 0.03 * inch))

        for idx, risk in enumerate(critical_risks, 1):
            # Triple-clean: use clean_text() + strict character preservation
            clean_risk = clean_text(str(risk))

            # Keep ONLY safe ASCII letters, numbers, spaces, and basic punctuation
            clean_risk = ''.join(c if (c.isalnum() or c in ' .,;:!?\'"()-') else '' for c in clean_risk)
            clean_risk = re.sub(r'\s+', ' ', clean_risk).strip()

            risk_parts = clean_risk.split(' - ')
            risk_param = risk_parts[0].strip()
            risk_impact = risk_parts[1].strip() if len(risk_parts) > 1 else ''

            if risk_param:
                story.append(Paragraph(
                    f"{idx}. <b>{risk_param}</b>",
                    body_style
                ))
                if risk_impact:
                    story.append(Paragraph(
                        f"Impact: {clean_text(risk_impact)}",
                        body_style
                    ))
                story.append(Spacer(1, 0.03 * inch))

        story.append(Spacer(1, 0.06 * inch))

    # ===== COMPANY ASSESSMENTS =====
    company_summaries = report_dict.get('company_summaries', [])
    if company_summaries:
        # Check if we need a page break (if content is too much)
        # Only add page break if there are many companies
        if len(company_summaries) > 5:
            story.append(PageBreak())
        else:
            story.append(Spacer(1, 0.08 * inch))

        story.append(Paragraph("RISK ASSESSMENT RESULTS", heading_style))
        story.append(Spacer(1, 0.08 * inch))

        for idx, company in enumerate(company_summaries, 1):
            company_name = clean_text(company.get('company_name', f'Company {idx}'))
            overall_status = company.get('overall_status', 'UNKNOWN')
            safe_params = company.get('safe_parameters', 0)
            unsafe_params = company.get('unsafe_parameters', 0)

            # Company header with status color
            color = '#28a745' if overall_status == 'SAFE' else '#dc3545'
            status_text = '✓ PASSED' if overall_status == 'SAFE' else '✗ FAILED'

            # Company title
            story.append(Paragraph(f"{idx}. {company_name}", subheading_style))

            # Overall status and parameter summary
            story.append(Paragraph(
                f"<b>Overall Assessment:</b> <font color='{color}'><b>{status_text}</b></font> | "
                f"<b>Parameters:</b> {safe_params} passed, {unsafe_params} require attention",
                body_style
            ))
            story.append(Spacer(1, 0.04 * inch))

            # Detailed parameter analysis
            failed_params = company.get('failed_parameters', [])

            if failed_params or safe_params > 0:
                story.append(Paragraph("<b>Parameter-by-Parameter Analysis:</b>", body_style))
                story.append(Spacer(1, 0.02 * inch))

                # Detailed elaboration for each parameter
                for param_idx, param in enumerate(failed_params, 1):
                    param_name = clean_text(param.get('name', 'Unknown'))
                    param_reason = clean_text(param.get('reason', 'No details'))

                    story.append(Paragraph(
                        f"<b>{param_idx}. {param_name}</b> <font color='#dc3545'>[UNSAFE]</font>",
                        body_style
                    ))
                    story.append(Paragraph(
                        f"Assessment: {param_reason}",
                        body_style
                    ))
                    story.append(Spacer(1, 0.02 * inch))

                # Add note for passed parameters
                if safe_params > 0:
                    story.append(Paragraph(
                        f"<b>Note:</b> {safe_params} additional parameter(s) met all mandate requirements and passed the assessment.",
                        body_style
                    ))
            else:
                story.append(Paragraph(
                    "<b>Assessment Summary:</b> All evaluated parameters meet mandate requirements. "
                    "This company demonstrates full compliance with the fund's risk criteria across all assessed dimensions.",
                    body_style
                ))

            # Investment recommendation based on status
            story.append(Spacer(1, 0.03 * inch))
            if overall_status == 'SAFE':
                story.append(Paragraph(
                    "<b>Investment Recommendation:</b> <font color='#28a745'>APPROVED - Company meets all mandate requirements</font>",
                    body_style
                ))
            else:
                story.append(Paragraph(
                    "<b>Investment Recommendation:</b> <font color='#dc3545'>REQUIRES REMEDIATION - Address failed parameters before investment</font>",
                    body_style
                ))

            story.append(Spacer(1, 0.06 * inch))

    # ===== KEY FINDINGS (at the end) =====
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("KEY FINDINGS & INSIGHTS", heading_style))
    key_findings = report_dict.get('key_findings', [])
    if key_findings:
        for i, finding in enumerate(key_findings, 1):
            # Triple-clean: use clean_text() + strict character preservation
            clean_finding = clean_text(str(finding))

            # Verify: keep ONLY safe ASCII letters, numbers, spaces, and basic punctuation
            # This prevents ANY Unicode from slipping through
            clean_finding = ''.join(c if (c.isalnum() or c in ' .,;:!?\'"()- ') else '' for c in clean_finding)

            # Normalize whitespace
            clean_finding = re.sub(r'\s+', ' ', clean_finding).strip()

            if clean_finding and len(clean_finding) > 2:
                story.append(Paragraph(f"{i}. {clean_finding}", body_style))
                story.append(Spacer(1, 0.03 * inch))
    else:
        story.append(Paragraph("No key findings identified.", body_style))
    story.append(Spacer(1, 0.1 * inch))

    # ===== FOOTER =====
    story.append(Spacer(1, 0.1 * inch))
    footer_text = (
        f"Risk Assessment Report | Generated on {report_dict.get('generated_at', 'N/A')} | "
        f"By Report Agent"
    )
    story.append(Paragraph(footer_text, styles['Normal']))

    # Build PDF with professional borders and page numbers
    doc.build(story, onFirstPage=BorderedCanvas(), onLaterPages=BorderedCanvas())
    pdf_buffer.seek(0)
    return pdf_buffer.getvalue()


# ============================================================================
# DATABASE DATA FETCHING FOR REPORT GENERATION
# ============================================================================

async def fetch_report_data_from_database(mandate_id: int, event_queue: Optional[Any] = None) -> Dict[str, Any]:
    """
    Fetch all relevant data from database based on mandate_id.

    Retrieves:
    - Mandate details from FundMandate
    - Sourced companies by fetching unique company_ids from Screening and RiskAnalysis tables
    - Screened companies from Screening table
    - Risk analysis results from RiskAnalysis table

    Args:
        mandate_id: ID of the fund mandate
        event_queue: Optional queue for streaming progress events

    Returns:
        Dictionary containing sourced_companies, screened_companies, risk_analysis, and mandate_details
    """
    try:
        from database.repositories.fundRepository import FundMandateRepository
        from database.repositories.companyRepository import CompanyRepository
        from database.repositories.screeningRepository import ScreeningRepository
        from database.repositories.riskAssessmentRepository import RiskAssessmentRepository
        from database.repositories.sourcingRepository import SourcingRepository
        from types import SimpleNamespace

        if event_queue:
            event_queue.put({
                'type': 'report_progress',
                'message': f'Fetching data for mandate {mandate_id}...',
                'timestamp': datetime.now().isoformat()
            })

        # Fetch mandate details using fetch_by_id
        mandate = await FundMandateRepository.fetch_by_id(mandate_id)
        if not mandate:
            print(f"[DB FETCH] Mandate {mandate_id} not found")
            mandate_details = None
        else:
            mandate_details = {
                'id': mandate.id,
                'legal_name': mandate.legal_name,
                'strategy_type': mandate.strategy_type,
                'vintage_year': mandate.vintage_year,
                'primary_analyst': mandate.primary_analyst,
                'processing_date': mandate.processing_date.strftime('%B %d, %Y') if mandate.processing_date else None,
                'description': mandate.description
            }
            print(f"[DB FETCH] ✓ Loaded mandate: {mandate.legal_name}")
            if event_queue:
                event_queue.put({
                    'type': 'report_progress',
                    'message': f'Loaded mandate: {mandate.legal_name}',
                    'timestamp': datetime.now().isoformat()
                })

        # Fetch screening and risk records using repository methods (used elsewhere in report)
        screened_records = await ScreeningRepository.get_screenings_by_mandate(mandate_id)
        risk_records = await RiskAssessmentRepository.get_results_by_mandate(mandate_id)

        # Attempt to load sourced companies from SourcingRepository first
        sourced_companies = []
        try:
            sourcings = await SourcingRepository.get_sourcings_by_mandate(mandate_id)
        except Exception as e:
            print(f"[DB FETCH] Warning: could not load sourcings for mandate {mandate_id}: {e}")
            sourcings = []

        if sourcings:
            for s in sourcings:
                try:
                    data = getattr(s, 'company_data', {}) or {}

                    # First try to get the canonical Company record (if present) so we can use its company_name
                    company_obj = None
                    try:
                        company_obj = await CompanyRepository.fetch_by_id(getattr(s, 'company_id', None))
                    except Exception:
                        company_obj = None

                    # Determine company_name using authoritative Company.company_name first
                    if company_obj and getattr(company_obj, 'company_name', None):
                        company_name = company_obj.company_name
                    else:
                        # Normalize common key names from sourcing.company_data
                        company_name = data.get('company_name') or data.get('Company') or data.get('name') or data.get(
                            'company')

                    sector = data.get('sector') or data.get('Sector') or data.get('industry_sector') or getattr(
                        company_obj, 'sector', None)
                    industry = data.get('industry') or data.get('Industry') or data.get('sub_industry') or getattr(
                        company_obj, 'industry', None)
                    country = data.get('country') or data.get('Country') or getattr(company_obj, 'country', None)

                    obj = SimpleNamespace(
                        id=getattr(s, 'company_id', None),
                        company_id=getattr(s, 'company_id', None),
                        company_name=company_name or f"Company {getattr(s, 'company_id', None)}",
                        sector=sector or 'N/A',
                        industry=industry or 'N/A',
                        country=country or 'N/A',
                        company_data=data,
                        selected_parameters=getattr(s, 'selected_parameters', None)
                    )
                    sourced_companies.append(obj)
                except Exception as e:
                    print(f"[DB FETCH] Warning: error mapping sourcing row to company object: {e}")
                    continue

            print(f"[DB FETCH] ✓ Loaded {len(sourced_companies)} sourced companies (from SourcingRepository)")
            if event_queue:
                event_queue.put({
                    'type': 'report_progress',
                    'message': f'Loaded {len(sourced_companies)} sourced companies (from SourcingRepository)',
                    'timestamp': datetime.now().isoformat()
                })
        else:
            # Fallback: derive company ids from screening and risk tables (legacy behavior)
            sourced_company_ids = set()
            for screening in screened_records:
                if screening.company_id:
                    sourced_company_ids.add(screening.company_id)
            for risk_analysis in risk_records:
                if risk_analysis.company_id:
                    sourced_company_ids.add(risk_analysis.company_id)

            # Fetch company details for each sourced company using fetch_by_id
            for company_id in sourced_company_ids:
                try:
                    company = await CompanyRepository.fetch_by_id(company_id)
                    if company:
                        sourced_companies.append(company)
                except Exception as e:
                    print(f"[DB FETCH] Warning: Could not fetch company {company_id}: {str(e)}")
                    continue

            print(f"[DB FETCH] ✓ Loaded {len(sourced_companies)} sourced companies (fallback)")
            if event_queue:
                event_queue.put({
                    'type': 'report_progress',
                    'message': f'Loaded {len(sourced_companies)} sourced companies (fallback)',
                    'timestamp': datetime.now().isoformat()
                })

        # Fetch screened companies using get_screenings_by_mandate
        screened_companies = screened_records
        print(f"[DB FETCH] ✓ Loaded {len(screened_companies)} screened companies")
        if event_queue:
            event_queue.put({
                'type': 'report_progress',
                'message': f'Loaded {len(screened_companies)} screened companies',
                'timestamp': datetime.now().isoformat()
            })

        # Fetch risk analysis results using get_results_by_mandate
        risk_analysis = risk_records
        print(f"[DB FETCH] ✓ Loaded {len(risk_analysis)} risk analysis results")
        if event_queue:
            event_queue.put({
                'type': 'report_progress',
                'message': f'Loaded {len(risk_analysis)} risk analysis results',
                'timestamp': datetime.now().isoformat()
            })

        return {
            'mandate_details': mandate_details,
            'sourced_companies': sourced_companies,
            'screened_companies': screened_companies,
            'risk_analysis': risk_analysis
        }

    except Exception as e:
        print(f"[DB FETCH ERROR] Error fetching report data: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# MAIN REPORT CREATION FUNCTION
# ============================================================================

def create_report_pdf(
        risk_results: Optional[List[Dict[str, Any]]] = None,
        output_path: Optional[str] = None,
        event_queue: Optional[Any] = None,
        mandate_id: Optional[int] = None
) -> tuple:
    """
    Creates a comprehensive risk assessment report from database records using mandate_id.

    IMPORTANT: This function now ALWAYS fetches data from the database using mandate_id.
    The risk_results parameter is IGNORED if mandate_id is provided.

    WORKFLOW:
    1. If mandate_id is provided → Fetch all data from database (sourcing, screening, risk_analysis tables)
    2. Generate comprehensive report with LLM analysis
    3. Create professional PDF with all mandate details
    4. Return (file_path, pdf_bytes, report_json_string)

    Args:
        risk_results: DEPRECATED - not used when mandate_id is provided
        output_path: Optional path to save PDF file
        event_queue: Optional queue for streaming progress events
        mandate_id: REQUIRED - ID of the fund mandate to fetch all data from

    Returns:
        Tuple of (file_path or None, pdf_bytes, report_json_string)
    """
    print(f"\n[REPORT GENERATION] Creating report from database (mandate_id={mandate_id})")

    if not mandate_id:
        raise Exception("mandate_id is required for report generation from database")

    # Delegate to the agent-based workflow which fetches from database
    return create_report_with_agent(
        mandate_id=mandate_id,
        output_path=output_path,
        event_queue=event_queue
    )


def save_report_pdf(
        risk_results: List[Dict[str, Any]],
        output_dir: str = './reports'
) -> str:
    """
    Convenience function to save a report PDF file.

    Args:
        risk_results: List of risk assessment results
        output_dir: Directory to save the PDF

    Returns:
        Path to the generated PDF file
    """
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir_path / f"risk_report_{timestamp}.pdf"

    file_path, _, _ = create_report_pdf(risk_results, str(output_file))
    return file_path