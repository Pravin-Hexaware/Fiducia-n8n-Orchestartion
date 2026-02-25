import json
import queue
import threading
import asyncio
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional, Annotated
import operator
from dotenv import load_dotenv
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from langchain_openai import AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.tools import tool
from langchain_core.messages import BaseMessage, HumanMessage, ToolMessage, SystemMessage
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode
import re

load_dotenv()

KEYVAULT_URI = "https://fstodevazureopenai.vault.azure.net/"
credential = DefaultAzureCredential()
kvclient = SecretClient(vault_url=KEYVAULT_URI, credential=credential)

# Retrieve Azure OpenAI configuration from Key Vault
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
# LANGGRAPH AGENT STATE
# ============================================================================

class RiskAssessmentState:
    """State for the risk assessment agent workflow"""
    messages: Annotated[list[BaseMessage], operator.add]
    companies: list[Dict[str, Any]]
    risk_parameters: Dict[str, str]
    all_results: list[Dict[str, Any]]
    current_company_index: int
    current_company_result: Optional[Dict[str, Any]]


# ============================================================================
# CLEAN EVENT STREAMING CALLBACK - MEANINGFUL THOUGHTS ONLY
# ============================================================================

class CleanEventCallback(BaseCallbackHandler):
    """
    Emits meaningful agent thinking and tool invocations without noise.
    Only sends substantial thoughts and tool usage events.
    Captures token usage from LLM responses.
    """

    def __init__(self, event_queue=None):
        self.event_queue = event_queue
        self.buffer = ""
        self.token_count = 0
        self.sentence_endings = {'.', '!', '?'}
        self.semantic_pauses = {',', ':', ';'}

    def is_meaningful_content(self, text: str) -> bool:
        """Validates content is meaningful analysis, not noise or JSON structure"""
        if not text or not text.strip():
            return False

        # Filter out meaningless patterns
        meaningless_patterns = ['||empty||', '....', '----', '====', '****', '||||', '    ', '\n\n\n']

        text_lower = text.lower()
        for pattern in meaningless_patterns:
            if pattern in text_lower:
                return False

        # Filter out JSON structure
        json_char_count = sum(1 for c in text if c in '{}[]:,"')
        total_chars = len(text.strip())
        json_ratio = json_char_count / total_chars if total_chars > 0 else 0

        if json_ratio > 0.3:
            return False

        if any(text.strip().startswith(indicator) for indicator in
               ['{', '[', '"status', '"company_name', '"parameter']):
            return False

        if not any(c.isalpha() for c in text):
            return False

        return True

    def on_llm_new_token(self, token: str, **kwargs) -> None:
        """Buffers tokens and emits meaningful complete thoughts"""
        self.buffer += token
        self.token_count += 1

        has_sentence_ending = any(ending in self.buffer for ending in self.sentence_endings)
        has_semantic_pause = any(pause in self.buffer for pause in self.semantic_pauses)

        should_emit = False

        if has_sentence_ending and self.token_count >= 50:
            should_emit = True
        elif has_semantic_pause and len(self.buffer.strip()) > 50 and self.token_count >= 50:
            should_emit = True
        elif self.token_count >= 75:
            if self.buffer.strip() and len(self.buffer.strip()) > 50:
                should_emit = True

        if should_emit:
            content = self.buffer.strip()
            if content and self.is_meaningful_content(content):
                if self.event_queue:
                    self.event_queue.put({
                        "type": "agent_thinking",
                        "content": content,
                        "timestamp": datetime.now().isoformat()
                    })
            self.buffer = ""
            self.token_count = 0

    def on_llm_end(self, response, **kwargs) -> None:
        """Flushes remaining meaningful content and captures token usage"""
        content = self.buffer.strip()
        if content and self.is_meaningful_content(content):
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
                "content": f"Using tool: {action.tool}",
                "timestamp": datetime.now().isoformat()
            })

    def on_tool_start(self, serialized: dict, input_str: str, **kwargs):
        """Tool is about to execute - stream immediately for progress"""
        tool_name = serialized.get("name", "unknown")
        print(f"\n[DEBUG] Tool starting: {tool_name}", flush=True)
        if self.event_queue:
            self.event_queue.put({
                "type": "tool_invocation",
                "tool": tool_name,
                "message": f"Invoking {tool_name}...",
                "timestamp": datetime.now().isoformat()
            })


def get_azure_llm(event_queue=None):
    """Initializes Azure OpenAI LLM with streaming enabled"""
    try:
        return AzureChatOpenAI(
            azure_deployment=DEPLOYMENT_NAME,
            openai_api_version=OPENAI_API_VERSION,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_key=GPT5_API_KEY,
            temperature=1,
            streaming=True,
            callbacks=[CleanEventCallback(event_queue=event_queue)]
        )
    except Exception as e:
        print(f"Error initializing Azure LLM: {str(e)}")
        raise e


def get_azure_llm_for_tokens():
    """Initializes Azure OpenAI LLM without streaming for reliable token capture"""
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


print("Authenticating with Azure KeyVault...")
llm = get_azure_llm()
print("Azure OpenAI LLM Initialized")

# ============================================================================
# GLOBAL STATE FOR ANALYSIS WORKFLOW
# ============================================================================

tool_output_capture = {"last_json": None}
event_queue_global = None
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


# ============================================================================
# DATABASE FETCHING FUNCTIONS
# ============================================================================

async def fetch_risk_data_from_database(fund_mandate_id: int, company_id: List[int],
                                        event_queue: Optional[Any] = None) -> Dict[str, Any]:
    """
    Fetch risk data from database based on fund_mandate_id and company_id.

    Fetches:
    - Mandate details from FundMandate
    - Mandate risk parameters from extracted_parameters.raw_response['mandate']['Risk Assessment of Investment Ideas']
    - Company details, company risks from Company table using company_id
    - Screening results for the companies

    Args:
        fund_mandate_id: ID of the fund mandate
        company_id: List of company IDs to fetch risks for
        event_queue: Optional queue for streaming progress events

    Returns:
        Dictionary containing:
        - mandate_details: Mandate information
        - mandate_risk_parameters: Risk parameters from mandate (what we're looking for)
        - companies: List of companies with their company_risks (what companies have)
    """
    try:
        from database.repositories.fundRepository import FundMandateRepository
        from database.repositories.companyRepository import CompanyRepository
        from database.repositories.screeningRepository import ScreeningRepository
        from database.repositories.ParametersRepository import ExtractedParametersRepository, RiskParametersRepository
        from types import SimpleNamespace

        if event_queue:
            event_queue.put({
                'type': 'report_progress',
                'message': f'Fetching risk data for mandate {fund_mandate_id}...',
                'timestamp': datetime.now().isoformat()
            })

        # Fetch mandate details
        mandate = await FundMandateRepository.fetch_by_id(fund_mandate_id)
        if not mandate:
            print(f"[DB FETCH] Mandate {fund_mandate_id} not found")
            mandate_details = None
        else:
            mandate_details = {
                'id': mandate.id,
                'legal_name': mandate.legal_name,
                'strategy_type': mandate.strategy_type,
                'vintage_year': mandate.vintage_year,
                'primary_analyst': mandate.primary_analyst,
                'description': mandate.description
            }
            print(f"[DB FETCH] ✓ Loaded mandate: {mandate.legal_name}")
            if event_queue:
                event_queue.put({
                    'type': 'report_progress',
                    'message': f'Loaded mandate: {mandate.legal_name}',
                    'timestamp': datetime.now().isoformat()
                })

        # Fetch MANDATE risk parameters from risk_parameters table
        mandate_risk_parameters = {}
        try:
            if mandate and mandate.extracted_parameters_id:
                # Fetch all risk parameters for this extracted_parameters_id
                risk_params = await RiskParametersRepository.fetch_by_extracted_parameters_id(
                    mandate.extracted_parameters_id)
                if risk_params:
                    # Convert list of RiskParameters objects to dict: {key: value}
                    for param in risk_params:
                        mandate_risk_parameters[param.key] = param.value

            if mandate_risk_parameters:
                print(
                    f"[DB FETCH] ✓ Loaded {len(mandate_risk_parameters)} mandate risk parameters from risk_parameters table")
                if event_queue:
                    event_queue.put({
                        'type': 'report_progress',
                        'message': f'Loaded {len(mandate_risk_parameters)} mandate risk parameters',
                        'timestamp': datetime.now().isoformat()
                    })
        except Exception as e:
            print(f"[DB FETCH] Warning: could not load mandate risk parameters: {e}")
            traceback.print_exc()

        # Fetch company details and COMPANY risks
        companies = []
        for cid in company_id:
            try:
                company = await CompanyRepository.fetch_by_id(cid)
                if company:
                    company_obj = SimpleNamespace(
                        id=company.id,
                        company_id=company.id,
                        company_name=company.company_name or f"Company {company.id}",
                        company_risks=company.risks or {},  # Company risks from Company.risks column
                        sector=company.sector or 'N/A',
                        industry=company.industry or 'N/A',
                        country=company.country or 'N/A'
                    )
                    companies.append(company_obj)
                else:
                    print(f"[DB FETCH] Warning: Company {cid} not found")
            except Exception as e:
                print(f"[DB FETCH] Warning: Error fetching company {cid}: {str(e)}")
                continue

        print(f"[DB FETCH] ✓ Loaded {len(companies)} companies with their risks")
        if event_queue:
            event_queue.put({
                'type': 'report_progress',
                'message': f'Loaded {len(companies)} companies with risks',
                'timestamp': datetime.now().isoformat()
            })

        return {
            'mandate_details': mandate_details,
            'mandate_risk_parameters': mandate_risk_parameters,
            'companies': companies
        }

    except Exception as e:
        print(f"[DB FETCH ERROR] Error fetching risk data: {str(e)}")
        traceback.print_exc()
        raise


# ============================================================================
# RISK ANALYSIS TOOL FOR LANGCHAIN AGENT
# ============================================================================

@tool
def analyze_company_risks(company_name: str, company_risks: str, mandate_risks: str) -> str:
    """
    Analyzes company risks against mandate requirements.
    Uses LLM to evaluate each risk parameter and provide overall investment verdict.

    Returns JSON with per-parameter analysis and overall assessment.
    """

    prompt = ChatPromptTemplate.from_template("""
### System Role

You are a Senior Risk Analyst at a Tier-1 Private Equity firm. Your objective is a strict binary compliance check: Do the identified risks of a target company align with our specific Mandate Requirements?

### Constraints

1. **Scope:** Evaluate ONLY the categories listed in MANDATE REQUIREMENTS.

2. **Exclusion:** If a risk exists in the COMPANY RISKS but is NOT in the MANDATE REQUIREMENTS, ignore it entirely.

3. **Binary Logic:** A parameter is SAFE only if the company risk profile meets or stays within the mandate threshold. Otherwise, it is UNSAFE.

4. **Overall Logic:** The overall status is SAFE if and only if ALL evaluated parameters are SAFE. If one or more fail, the status is UNSAFE.

### Inputs

- **Target Company:** {company_name}

- **Company Risk Profile:** {company_risks}

- **Mandate Requirements:** {mandate_risks}

### Output Instructions

Return a strictly valid JSON object. Do not include markdown formatting, "```json" tags, or any conversational preamble.

### JSON Schema

{{
    "company_name": "{company_name}",
    "parameter_analysis": {{
        "{{Category_Name}}": {{
            "status": "SAFE | UNSAFE",
            "reason": "Max 15 words explaining the specific alignment or breach."
        }}
    }},
    "overall_assessment": {{
        "status": "SAFE | UNSAFE",
        "reason": "Max 20 words summarizing the investment viability based solely on the mandate."
    }}
}}
    """)

    try:
        # Use non-streaming LLM to capture tokens reliably
        llm_instance = get_azure_llm_for_tokens()

        # Format prompt and invoke LLM directly to get token usage
        formatted_prompt = prompt.format(
            company_name=company_name,
            company_risks=company_risks,
            mandate_risks=mandate_risks
        )

        response = llm_instance.invoke(formatted_prompt)

        # Extract token usage from response metadata
        if hasattr(response, 'response_metadata') and response.response_metadata:
            usage = response.response_metadata.get("token_usage", {})
            if usage and (usage.get('prompt_tokens', 0) > 0 or usage.get('completion_tokens', 0) > 0):
                print(
                    f"[TOKEN] Prompt: {usage.get('prompt_tokens', 0)}, Completion: {usage.get('completion_tokens', 0)}")
                accumulate_tokens(usage)
            else:
                print(f"[TOKEN] No usage data in response_metadata: {usage}")
        else:
            print(f"[TOKEN] No response_metadata found. Response type: {type(response)}")

        response_text = response.content if hasattr(response, 'content') else str(response)
        response_text = re.sub(r'```(?:json)?\s*\n?', '', response_text)
        response_text = response_text.strip()

        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}')

        if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
            json_str = response_text[start_idx:end_idx + 1]
            result = json.loads(json_str)
        else:
            result = json.loads(response_text)

        required_fields = ['company_name', 'parameter_analysis', 'overall_assessment']
        if not all(k in result for k in required_fields):
            raise ValueError("Missing required fields in response")

        if not isinstance(result['overall_assessment'], dict):
            raise ValueError("overall_assessment must be an object")
        if 'status' not in result['overall_assessment'] or 'reason' not in result['overall_assessment']:
            raise ValueError("overall_assessment must contain status and reason")

        result['company_name'] = company_name
        result['overall_assessment']['status'] = result['overall_assessment']['status'].upper()

        for param, analysis in result.get('parameter_analysis', {}).items():
            if 'status' in analysis:
                analysis['status'] = analysis['status'].upper()

        print(f"\nAnalysis complete for {company_name}")
        print(f"Overall Status: {result['overall_assessment']['status']}")

        tool_output_capture["last_json"] = result
        return json.dumps(result)

    except Exception as e:
        print(f"Error in analyze_company_risks: {str(e)}")
        result = {
            "company_name": company_name,
            "parameter_analysis": {},
            "overall_assessment": {
                "status": "UNSAFE",
                "reason": "Analysis failed due to error"
            }
        }
        tool_output_capture["last_json"] = result
        return json.dumps(result)


# ============================================================================
# LANGGRAPH AGENT SETUP
# ============================================================================

def create_risk_assessment_agent(event_queue=None):
    """
    Creates a LangGraph-based risk assessment agent.

    This agent orchestrates the risk assessment workflow using LangGraph pattern:
    1. Agent node processes companies one at a time
    2. Tool node executes the analyze_company_risks tool
    3. Loop continues until all companies are analyzed

    Args:
        event_queue: Optional queue for streaming progress events

    Returns:
        Compiled LangGraph workflow
    """

    tools = [analyze_company_risks]
    llm_with_streaming = get_azure_llm(event_queue=event_queue)
    llm_with_tools = llm_with_streaming.bind_tools(tools, strict=False)

    system_prompt = """Agent Name: risk_assessment_investment_ideas_agent
Description: This agent manages the Risk Assessment of Investment Ideas sub-process within the Research and Idea Generation process for the Fund Mandate capability. 
It identifies and quantifies potential downsides, including liquidity risk, volatility, and alignment with mandate-specific risk constraints. 
Trigger this agent to vet proposed investment ideas against risk frameworks before they are finalized in the idea generation phase.

"""

    agent_prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("user", "{input}"),
        ("assistant", "{agent_scratchpad}")
    ])

    # Define the agent node
    def agent_node(state: dict, config=None):
        """LLM follows REACT format with full message history"""

        input_text = state.get("current_task", "Analyze the following company against mandate requirements")

        # STEP 1: Call LLM WITHOUT tools to get thinking text
        llm_no_tools = get_azure_llm(event_queue=event_queue)

        thinking_response = (agent_prompt | llm_no_tools).invoke(
            {
                "input": input_text,
                "agent_scratchpad": format_messages_for_scratchpad(state.get("messages", []))
            },
            config=config,
        )

        # STEP 2: Call LLM WITH tools to get tool calls
        tool_response = (agent_prompt | llm_with_tools).invoke(
            {
                "input": input_text,
                "agent_scratchpad": format_messages_for_scratchpad(state.get("messages", []))
            },
            config=config,
        )

        return {
            "messages": [thinking_response, tool_response],
            "current_company_result": None
        }

    # Define the tool node
    def tool_node_handler(state: dict, config=None):
        """Execute tools and update results"""
        messages = state.get("messages", [])

        # Find the last AI message with tool calls
        ai_message = None
        for msg in reversed(messages):
            if hasattr(msg, 'tool_calls') and msg.tool_calls:
                ai_message = msg
                break

        if not ai_message or not ai_message.tool_calls:
            return {"messages": [], "current_company_result": None}

        # Execute tool
        tool_call = ai_message.tool_calls[0]
        tool_name = tool_call['name']
        tool_args = tool_call['args']

        # Execute the tool
        parsed_result = None
        tool_result = ""

        try:
            if tool_name == 'analyze_company_risks':
                tool_result = analyze_company_risks.invoke(tool_args)
                # Parse result and store in state
                parsed_result = json.loads(tool_result)
                state["current_company_result"] = parsed_result
            else:
                tool_result = f"Unknown tool: {tool_name}"
                parsed_result = {"error": "Unknown tool"}
        except Exception as tool_error:
            print(f"[TOOL_ERROR] Exception executing tool: {str(tool_error)}")
            # Ensure we always have a result, even if tool fails
            tool_result = json.dumps({
                "company_name": tool_args.get("company_name", "Unknown"),
                "parameter_analysis": {},
                "overall_assessment": {
                    "status": "UNSAFE",
                    "reason": f"Tool execution failed: {str(tool_error)}"
                }
            })
            parsed_result = json.loads(tool_result)
            state["current_company_result"] = parsed_result

        # Emit analysis_complete event
        if event_queue and parsed_result:
            overall_status = parsed_result.get('overall_assessment', {}).get('status', 'UNKNOWN')
            event_queue.put({
                "type": "analysis_complete",
                "company_name": parsed_result.get('company_name', 'Unknown'),
                "overall_result": overall_status,
                "timestamp": datetime.now().isoformat()
            })

        # Return tool message and signal completion
        return {
            "messages": [ToolMessage(content=tool_result, tool_call_id=tool_call['id'])],
            "current_company_result": parsed_result,
            "_tool_executed": True  # Flag to signal workflow should end
        }

    def should_continue(state: dict):
        """Determine if we should continue or end"""
        # If tool was just executed, end the workflow
        if state.get("_tool_executed"):
            return END

        messages = state.get("messages", [])
        last_message = messages[-1] if messages else None

        # Only continue if last message has tool calls
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            return "tools"

        # Otherwise, end workflow
        return END

    # Build the LangGraph workflow
    workflow = StateGraph(dict)

    workflow.add_node("agent", agent_node)
    workflow.add_node("tools", tool_node_handler)

    workflow.add_edge(START, "agent")
    workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    workflow.add_edge("tools", END)  # After tools execute, END the workflow (no loop back)

    graph = workflow.compile()

    return graph


def format_messages_for_scratchpad(messages: list) -> str:
    """Format messages for the agent scratchpad"""
    scratchpad = ""
    for msg in messages:
        if hasattr(msg, 'content'):
            scratchpad += f"{msg.content}\n"
    return scratchpad


# ============================================================================
# MAIN ANALYSIS FUNCTION - REAL-TIME EVENT STREAMING
# ============================================================================

def run_risk_assessment_sync(data: Dict[str, Any], event_queue=None, fund_mandate_id: Optional[int] = None) -> List[
    Dict[str, Any]]:
    """
    Executes risk assessment for multiple companies using LangGraph agent.
    Streams all events in real-time via event_queue for WebSocket delivery.

    NEW MODE (Recommended):
    Args:
        data: Contains 'company_id' list (array of company IDs from Screening table)
        event_queue: Queue to put real-time streaming events
        fund_mandate_id: REQUIRED - ID of the fund mandate for database fetching and persistence

    LEGACY MODE (Deprecated):
    Args:
        data: Contains 'companies' list with full payload and 'risk_parameters' dictionary
        event_queue: Queue to put real-time streaming events
        fund_mandate_id: Optional ID of the fund mandate for database persistence

    Returns:
        List of analysis results with verdicts for each company
    """

    set_event_queue_global(event_queue)
    reset_token_usage()

    # Check if using NEW MODE (fund_mandate_id + company_id) or LEGACY MODE (full payload)
    use_new_mode = fund_mandate_id is not None and 'company_id' in data and isinstance(data.get('company_id'), list)

    if use_new_mode:
        # NEW MODE: Fetch data from database
        print(f"\n[AGENT] NEW MODE: Fetching data from database (mandate_id={fund_mandate_id})")

        company_id = data.get('company_id', [])

        if not company_id:
            raise ValueError("Company ID list cannot be empty")

        try:
            # Fetch data from database asynchronously
            db_data = asyncio.run(fetch_risk_data_from_database(fund_mandate_id, company_id, event_queue))

            companies = []
            for company_obj in db_data.get('companies', []):
                # Convert SimpleNamespace to dict format expected by analyze_company_risks tool
                companies.append({
                    'Company': company_obj.company_name,
                    'Company_id': company_obj.company_id,
                    'Risks': company_obj.company_risks  # Use company_risks from company object
                })

            risk_parameters = db_data.get('mandate_risk_parameters', {})  # Get mandate risk parameters
            mandate_details = db_data.get('mandate_details', {})

        except Exception as e:
            print(f"[AGENT ERROR] Failed to fetch data from database: {str(e)}")
            raise ValueError(f"Failed to fetch data from database: {str(e)}")
    else:
        # LEGACY MODE: Use payload data directly
        print(f"\n[AGENT] LEGACY MODE: Using provided payload data")

        companies = data.get('companies', [])
        risk_parameters = data.get('risk_parameters', {})

        if not companies:
            raise ValueError("Companies list cannot be empty")
        if not risk_parameters:
            raise ValueError("Risk parameters cannot be empty")

    print(f"Starting risk assessment for {len(companies)} companies...")
    print(f"[AGENT] Database persistence: {'ENABLED' if fund_mandate_id else 'DISABLED'}")

    if event_queue:
        event_queue.put({
            "type": "session_start",
            "message": "Risk Assessment Agent initialized",
            "companies_count": len(companies),
            "timestamp": datetime.now().isoformat()
        })

    # Create the LangGraph agent (same interface as before)
    agent_graph = create_risk_assessment_agent(event_queue=event_queue)
    mandate_json = json.dumps(risk_parameters, indent=2)

    all_results = []

    for i, company in enumerate(companies, 1):
        try:
            company_name = company.get('Company') or company.get('Company ') or f'Company_{i}'
            company_id = company.get('Company_id')  # Get company_id for NEW MODE
            company_risks = company.get('Risks', {})
            company_risks_json = json.dumps(company_risks, indent=2)

            print(f"\nProcessing {company_name}...")

            # Emit company_analysis_start event
            if event_queue:
                event_queue.put({
                    "type": "company_analysis_start",
                    "company_name": company_name,
                    "company_id": company_id,
                    "timestamp": datetime.now().isoformat()
                })

            tool_output_capture["last_json"] = None

            task = f"""
            Analyze the following company against mandate requirements:

            Company Name: {company_name}
            Company Risks: {company_risks_json}
            Mandate Requirements: {mandate_json}

            Use the analyze_company_risks tool to perform the analysis.
            """

            # Invoke the LangGraph agent
            state = {
                "messages": [HumanMessage(content=task)],
                "current_task": task,
                "current_company_result": None
            }

            result = agent_graph.invoke(state)

            # Extract result from the state (primary source)
            result_data = result.get("current_company_result")

            if result_data:
                all_results.append(result_data)
                overall_status = result_data.get('overall_assessment', {}).get('status', 'UNKNOWN')
                print(f"Result for {result_data['company_name']}: {overall_status}")

                if event_queue:
                    event_queue.put({
                        "type": "analysis_complete",
                        "company_name": result_data['company_name'],
                        "overall_result": overall_status,
                        "timestamp": datetime.now().isoformat()
                    })
            else:
                # Fallback to tool_output_capture if state doesn't have result
                if tool_output_capture["last_json"]:
                    result_data = tool_output_capture["last_json"]
                    all_results.append(result_data)
                    print(
                        f"Result for {result_data.get('company_name', 'Unknown')}: {result_data.get('overall_assessment', {}).get('status', 'UNKNOWN')}")
                else:
                    raise ValueError("Tool did not produce output")

        except Exception as e:
            print(f"Error processing {company_name}: {str(e)}")
            error_result = {
                "company_name": company_name,
                "overall_assessment": {
                    "status": "UNSAFE",
                    "reason": "Analysis failed"
                },
                "parameter_analysis": {}
            }
            all_results.append(error_result)

            if event_queue:
                event_queue.put({
                    "type": "analysis_complete",
                    "company_name": company_name,
                    "overall_result": "UNSAFE",
                    "timestamp": datetime.now().isoformat()
                })

    print(f"\nRisk Assessment completed for {len(all_results)} companies")
    print(
        f"[TOKEN SUMMARY] Total tokens used - Prompt: {token_usage['prompt_tokens']}, Completion: {token_usage['completion_tokens']}")

    if event_queue:
        # Transform results to replace overall_assessment with overall_result
        transformed_results = []
        for result in all_results:
            transformed = {
                "company_name": result.get('company_name'),
                "parameter_analysis": result.get('parameter_analysis', {}),
                "overall_result": result.get('overall_assessment', {}).get('status', 'UNKNOWN')
            }
            transformed_results.append(transformed)

        event_queue.put({
            "type": "session_complete",
            "status": "success",
            "message": "Risk Assessment Agent session finished!",
            "companies_analyzed": len(all_results),
            "results": transformed_results,
            "token_usage": token_usage,
            "timestamp": datetime.now().isoformat()
        })
        event_queue.put(None)

    return all_results