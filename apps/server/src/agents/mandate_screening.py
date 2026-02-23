# import os
# import json
# import re
# import asyncio
# import sys
# from io import StringIO
# from typing import Optional, List, Any, Dict
# from crewai import Agent, Task, Crew, Process, LLM
# from crewai.tools import BaseTool
# from dotenv import load_dotenv
# from fastapi import WebSocket
# from azure.keyvault.secrets import SecretClient
# from azure.identity import DefaultAzureCredential
#
# load_dotenv()
#
#
# KEY_VAULT_NAME = "fstodevazureopenai"
# KEY_VAULT_URL = f"https://{KEY_VAULT_NAME}.vault.azure.net/"
#
#
# SECRETS_MAP = {
#     "api_key": "llm-api-key",
#     "endpoint": "llm-base-endpoint",
#     "deployment": "llm-41",
#     "api_version": "llm-41-version"
# }
#
# # Global LLM config
# llm_config = None
#
#
# def get_secrets_from_key_vault():
#     """Retrieve LLM secrets from Azure Key Vault"""
#     try:
#
#         credential = DefaultAzureCredential()
#         kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
#
#         secrets = {}
#         for key, secret_name in SECRETS_MAP.items():
#             try:
#                 secret_value = kv_client.get_secret(secret_name).value
#                 secrets[key] = secret_value
#
#             except Exception as e:
#                 print(f"  ❌ Failed to retrieve '{secret_name}': {e}")
#                 raise
#
#         return secrets
#
#     except Exception as e:
#         raise
#
#
# def initialize_azure_llm_config():
#     """Initialize Azure OpenAI LLM config for CrewAI"""
#     global llm_config
#
#     try:
#
#         # Get secrets from KeyVault
#         secrets = get_secrets_from_key_vault()
#
#
#         # Set environment variables for litellm/Azure
#         os.environ["AZURE_API_KEY"] = secrets['api_key']
#         os.environ["AZURE_API_BASE"] = secrets['endpoint']
#         os.environ["AZURE_API_VERSION"] = secrets['api_version']
#
#         llm_config = {
#             "model": f"azure/{secrets['deployment']}",
#             "api_key": secrets['api_key'],
#             "api_base": secrets['endpoint'],
#             "api_version": secrets['api_version'],
#             "temperature": 0.3,
#             "max_tokens": 16384
#         }
#
#
#         return llm_config
#
#     except Exception as e:
#
#         raise
#
#
# try:
#     llm_config = initialize_azure_llm_config()
# except Exception as e:
#
#     raise
#
#
# # ============================================================================
# # UPDATED: REAL-TIME EVENT CAPTURE FOR MULTIPLE TOOLS - WITH MULTIPLE AGENT THINKING
# # ============================================================================
#
# class RealtimeEventCapture:
#     """Capture events in real-time from stdout - HANDLES MULTIPLE TOOL EXECUTIONS WITH SEPARATE THINKING"""
#
#     def __init__(self, original_stdout, callback, loop):
#         self.original_stdout = original_stdout
#         self.callback = callback
#         self.loop = loop
#         self.buffer = ""
#
#         # Track events
#         self.reasoning_sent = False
#         self.thought_sent = False
#         self.tools_executed = []
#         self.last_tool_thinking = {}  # Track thinking for each tool
#
#     def write(self, text: str) -> None:
#         """Write to terminal AND check for events"""
#         self.original_stdout.write(text)
#         self.original_stdout.flush()
#         self.buffer += text
#         self._check_events_in_order()
#
#     def _clean_text(self, text: str) -> str:
#         """Remove non-ASCII characters and special Unicode"""
#         cleaned = text.replace('\xa0', ' ')
#         cleaned = cleaned.replace('\n', ' ')
#         cleaned = cleaned.replace('\r', ' ')
#         cleaned = re.sub(r'[^\x20-\x7E]', '', cleaned)
#         cleaned = re.sub(r'\s+', ' ', cleaned)
#         return cleaned.strip()
#
#     def _check_events_in_order(self) -> None:
#         """Check for events - handles multiple tool executions with separate thinking"""
#         try:
#             # EVENT 1: Reasoning Plan
#             if ("Reasoning Plan" in self.buffer and not self.reasoning_sent):
#                 reasoning_match = re.search(
#                     r'Reasoning Plan(.*?)(?=Agent:|$)',
#                     self.buffer,
#                     re.DOTALL
#                 )
#
#                 if reasoning_match:
#                     reasoning_text = "Reasoning Plan" + reasoning_match.group(1)
#                     reasoning_text = self._clean_text(reasoning_text)
#                     if len(reasoning_text) >= 20:
#                         self._send_event_safe(self.callback.on_reasoning_plan(reasoning_text))
#                         self.reasoning_sent = True
#
#             # EVENT 2 & MULTIPLE: Agent Thinking - CAPTURE FOR EACH TOOL
#             flags = re.IGNORECASE
#             if self.reasoning_sent:
#                 # Find ALL "Using Tool:" occurrences
#                 tool_patterns = list(re.finditer(r'Using\s*Tool:?\s*(\S+)', self.buffer, flags))
#
#                 for idx, tool_match in enumerate(tool_patterns):
#                     tool_name = tool_match.group(1).strip()
#
#                     # Skip if we already processed thinking for this tool
#                     if tool_name in self.last_tool_thinking:
#                         continue
#
#                     # Look backwards from tool position to find preceding Thought/Agent
#                     tool_pos = tool_match.start()
#                     preceding_buffer = self.buffer[:tool_pos]
#
#                     # Find the LAST Agent, Thought, and Action before this tool
#                     agent_match = None
#                     thought_match = None
#                     action_match = None
#
#                     # Search backwards from tool position
#                     for prev_agent in re.finditer(r'Agent:\s*([^\n]+)', preceding_buffer, flags):
#                         agent_match = prev_agent
#                     for prev_thought in re.finditer(r'Thought:\s*([^\n]+)', preceding_buffer, flags):
#                         thought_match = prev_thought
#                     for prev_action in re.finditer(r'Action:\s*([^\n]+)', preceding_buffer, flags):
#                         action_match = prev_action
#
#                     # Only send if we have agent and thought
#                     if agent_match and thought_match:
#                         agent = self._clean_text(agent_match.group(1))
#                         thought = self._clean_text(thought_match.group(1))
#                         action = self._clean_text(action_match.group(1)) if action_match else None
#
#                         parts = [f"Agent: {agent}", "", f"Thought: {thought}"]
#                         if action:
#                             parts += ["", f"Action: {action}"]
#                         parts += ["", f"Using Tool: {tool_name}"]
#
#                         thinking_msg = "\n".join(parts)
#                         self._send_event_safe(self.callback.on_agent_thinking(thinking_msg))
#                         self.last_tool_thinking[tool_name] = True
#
#                         self.original_stdout.write(f"\n✓ [EVENT 2.{len(self.last_tool_thinking)}] Agent thinking sent for {tool_name}\n")
#                         self.original_stdout.flush()
#
#             # EVENT 3 & 4: MULTIPLE TOOL EXECUTIONS
#             if self.reasoning_sent:
#                 tool_patterns = re.finditer(r'Using Tool:\s*(\S+)', self.buffer, flags)
#
#                 for match in tool_patterns:
#                     tool_name = match.group(1).strip()
#
#                     # Skip if we already sent event for this tool
#                     if tool_name in self.tools_executed:
#                         continue
#
#                     # Send TOOL START event
#                     self._send_event_safe(self.callback.on_tool_start(tool_name))
#                     self.original_stdout.write(f"\n✓ [EVENT 3] Tool start sent: {tool_name}\n")
#                     self.original_stdout.flush()
#
#                     self.tools_executed.append(tool_name)
#
#                 # Check for tool completion
#                 for tool_name in self.tools_executed:
#                     if (f"[{tool_name}]" in self.buffer or
#                         "Tool Result:" in self.buffer or
#                         "companies passed" in self.buffer.lower()):
#
#                         result_match = re.search(
#                             r'(\d+)\s*(?:companies?|passed)',
#                             self.buffer,
#                             re.IGNORECASE
#                         )
#                         count = result_match.group(1) if result_match else "companies"
#
#                         self._send_event_safe(self.callback.on_tool_end(
#                             tool_name,
#                             f"{count} passed {tool_name} criteria"
#                         ))
#                         self.original_stdout.write(f"\n✓ [EVENT 4] Tool end sent: {tool_name}\n")
#                         self.original_stdout.flush()
#
#         except Exception as e:
#             self.original_stdout.write(f"\n⚠️ Event capture error: {e}\n")
#             self.original_stdout.flush()
#
#     def _send_event_safe(self, coro):
#         """Safely send coroutine to event loop from thread"""
#         try:
#             if self.loop and self.loop.is_running():
#                 asyncio.run_coroutine_threadsafe(coro, self.loop)
#         except Exception:
#             pass
#
#     def flush(self) -> None:
#         self.original_stdout.flush()
#
#     def get_buffer(self) -> str:
#         return self.buffer
#
#
# class WebSocketStreamingCallback:
#     """Stream events to WebSocket with content cleaning"""
#
#     def __init__(self, websocket: WebSocket):
#         self.websocket = websocket
#         self.step_count = 0
#         self.tools_started = 0  # Track tool execution count
#
#     def _clean_content(self, content: str) -> str:
#         """Remove non-ASCII, ANSI codes, and special Unicode characters"""
#         ansi_escape_pattern = r'\x1b\[[0-9;]*m|\[0m|\[32m|\[37m'
#         cleaned = re.sub(ansi_escape_pattern, '', content)
#
#         cleaned = cleaned.replace('\xa0', ' ')
#         cleaned = cleaned.replace('\u200b', '')
#         cleaned = cleaned.replace('\r', '')
#         cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', cleaned)
#         cleaned = re.sub(r'[^\x20-\x7E\n]', '', cleaned)
#         cleaned = re.sub(r'  +', ' ', cleaned)
#         cleaned = re.sub(r'\n\n+', '\n', cleaned)
#
#         return cleaned.strip()
#
#     async def send_event(self, event_type: str, content: str) -> None:
#         """Send event to WebSocket"""
#         try:
#             self.step_count += 1
#             cleaned_content = self._clean_content(content)
#
#             message = {
#                 "type": event_type,
#                 "content": cleaned_content,
#                 "step": self.step_count
#             }
#             print(f"\n[STEP {self.step_count}] Sending: {event_type}")
#             await self.websocket.send_json(message)
#             await asyncio.sleep(0.3)
#         except Exception as e:
#             print(f"WebSocket error: {e}")
#
#     async def on_agent_initialized(self) -> None:
#         content = """STEP 1: Agent Initialized
#
# Bottom-Up Fundamental Analysis agent is ready
# Initializing screening process..."""
#         await self.send_event("step_1", content)
#
#     async def on_reasoning_plan(self, plan: str) -> None:
#         content = f"""STEP 2A: Reasoning Plan
#
# {plan}"""
#         await self.send_event("step_2a", content)
#
#     async def on_agent_thinking(self, thought: str) -> None:
#         content = f"""STEP 2B: Bottom-Up Fundamental Analysis Agent Thinking
#
# {thought}"""
#         await self.send_event("step_2b", content)
#
#     async def on_tool_start(self, tool_name: str) -> None:
#         """Send tool start event with SPECIFIC TOOL NAME"""
#         self.tools_started += 1
#         # Format tool name nicely
#         tool_display = tool_name.replace("_", " ").title()
#
#         content = f"""STEP 3.{self.tools_started}: Tool Execution Started
#
# Executing: {tool_display}
# Screening companies against mandate parameters..."""
#         await self.send_event(f"step_3_{self.tools_started}", content)
#
#     async def on_tool_end(self, tool_name: str, output: str) -> None:
#         """Send tool end event with SPECIFIC TOOL NAME and results"""
#         tool_display = tool_name.replace("_", " ").title()
#
#         content = f"""STEP 4.{self.tools_started}: Tool Completed
#
# {tool_display} executed successfully
# Result: {output}"""
#         await self.send_event(f"step_4_{self.tools_started}", content)
#
#     async def on_screening_progress(self, message: str) -> None:
#         content = f"""STEP 5: Results Processing
#
# {message}"""
#         await self.send_event("step_5", content)
#
#     async def on_agent_finish(self, result: str) -> None:
#         content = f"""STEP 6: Bottom-Up Fundamental Analysis Agent Task Completed
#
# {result}"""
#         await self.send_event("step_6", content)
#
#     async def on_final_output(self, output: str) -> None:
#         content = f"""STEP 7: Final Output Ready
#
# {output}"""
#         await self.send_event("step_7", content)
#
#     async def on_error(self, error: str) -> None:
#         await self.send_event("error", f"Error: {error}")
#
#
# def parse_constraint(constraint_str: str) -> tuple:
#     """Parse constraint - handles $, %, B, M, T and converts all thresholds into MILLIONS."""
#     try:
#         constraint_str = str(constraint_str).strip()
#         constraint_str = constraint_str.replace("&amp;gt;", "&gt;").replace("&amp;lt;", "&lt;")
#
#         # Special cases
#         if constraint_str.lower() == "positive":
#             return ">", 0
#
#         if constraint_str.lower() in ["", "-", "na", "n/a", "none", "null"]:
#             return "skip", 0
#
#
#         if "not required" in constraint_str.lower():
#             return "skip", 0
#
#         constraint_str = constraint_str.replace("&amp;amp;gt;", "&gt;").replace("&amp;amp;lt;", "&lt;")
#
#         # Extract operator and number
#         match = re.search(r'([><]=?|==|!=)\s*([\d.]+)', constraint_str)
#         if not match:
#             return ">", 0
#
#         operator = match.group(1)
#         threshold = float(match.group(2))
#
#         # Identify units
#         has_dollar = '$' in constraint_str
#         has_billion = 'B' in constraint_str.upper() and 'M' not in constraint_str.upper()
#         has_trillion = 'T' in constraint_str.upper()
#         has_million = 'M' in constraint_str.upper()
#         has_percent = '%' in constraint_str
#
#         # Convert currency amounts → millions
#         if has_dollar:
#             if has_billion:
#                 threshold = threshold * 1000
#             elif has_trillion:
#                 threshold = threshold * 1000000
#             elif not has_million:
#                 threshold = threshold / 1_000_000
#
#         # Convert % → decimal
#         if has_percent:
#             if threshold > 1:
#                 threshold = threshold / 100
#
#         return operator, threshold
#
#     except Exception as e:
#         return ">", 0
#
#
# def get_company_value(company: dict, param_name: str) -> Optional[float]:
#     """Get numeric value from company - ALL VALUES IN MILLIONS"""
#     try:
#         param_lower = param_name.lower()
#
#         # Handle REVENUE
#         if param_lower == "revenue":
#             revenue = company.get("Revenue")
#             if revenue is None:
#                 return None
#             return parse_value(revenue)
#
#         # Handle NET INCOME
#         if param_lower == "net_income":
#             net_income = company.get("Net Income")
#             if net_income is None:
#                 return None
#             return parse_value(net_income)
#
#         # Handle MARKET CAP
#         if param_lower == "market_cap":
#             market_cap = company.get("Market Cap")
#             if market_cap is None:
#                 return None
#             return parse_value(market_cap)
#
#         # Handle EBITDA
#         if param_lower == "ebitda":
#             ebitda_raw = company.get("EBITDA")
#             if ebitda_raw is None:
#                 return None
#             return parse_value(ebitda_raw)
#
#         # Handle GROSS PROFIT MARGIN
#         if param_lower == "gross_profit_margin":
#             gpm = company.get("Gross Profit Margin")
#             if gpm is None:
#                 return None
#             parsed = parse_value(gpm)
#             if parsed is None:
#                 return None
#             if parsed > 1:
#                 parsed = parsed / 100
#             return parsed
#
#         # Handle RETURN ON EQUITY
#         if param_lower == "return_on_equity":
#             roe = company.get("Return on Equity")
#             if roe is None:
#                 return None
#             parsed = parse_value(roe)
#             if parsed is None:
#                 return None
#             if parsed > 1:
#                 parsed = parsed / 100
#             return parsed
#
#         # Standard field mapping
#         field_map = {
#             "debt_to_equity": ["Debt / Equity"],
#             "pe_ratio": ["P/E Ratio"],
#             "price_to_book": ["Price/Book"],
#             "dividend_yield": ["Dividend Yield"]
#         }
#
#         fields = field_map.get(param_lower, [param_name])
#
#         for field in fields:
#             if field in company:
#                 value = company[field]
#                 if value is None:
#                     continue
#                 parsed = parse_value(value)
#                 if parsed is not None:
#                     return parsed
#
#         return None
#     except Exception:
#         return None
#
#
# def parse_value(value: Any) -> Optional[float]:
#     """Parse various value formats (B, M, T, %) - RETURNS VALUE IN MILLIONS"""
#     try:
#         if value is None:
#             return None
#
#         if isinstance(value, (int, float)):
#             return float(value)
#
#         if isinstance(value, str):
#             value_str = str(value).strip()
#
#             # Remove newlines, %, $, commas
#             value_str = value_str.replace("\n", "").replace("%", "").replace("$", "").replace(",", "")
#             # Remove dots before B, M, T (e.g., "244.12B" -> "24412B")
#             value_str = re.sub(r'(\d)\.(\d+)([BMT])', r'\1\2\3', value_str)
#
#             # Handle T (trillions) -> convert to millions
#             if 'T' in value_str.upper():
#                 value_str = value_str.upper().replace('T', '').strip()
#                 return float(value_str) * 1000000
#
#             # Handle B (billions) -> convert to millions
#             if 'B' in value_str.upper():
#                 value_str = value_str.upper().replace('B', '').strip()
#                 return float(value_str) * 1000
#
#             # Handle M (millions)
#             if 'M' in value_str.upper():
#                 value_str = value_str.upper().replace('M', '').strip()
#                 return float(value_str)
#
#             # Plain number
#             if value_str:
#                 return float(value_str)
#
#         return None
#     except Exception:
#         return None
#
#
# def screen_companies_simple(mandate_parameters: dict, companies: list) -> list:
#     """Screen companies against mandate parameters"""
#     passed_companies = []
#
#     try:
#         if not mandate_parameters or not companies:
#             return passed_companies
#
#         for company in companies:
#             try:
#                 company_name = company.get("Company ", company.get("Company", "Unknown")).strip()
#                 sector = company.get("Sector", "Unknown").strip()
#
#                 all_passed = True
#                 reasons = []
#
#                 for param_name, constraint_str in mandate_parameters.items():
#                     # Skip non-required fields
#                     if "not required" in str(constraint_str).lower():
#                         continue
#
#                     operator, threshold = parse_constraint(constraint_str)
#
#                     # Skip if operator is "skip"
#                     if operator == "skip":
#                         continue
#
#                     company_value = get_company_value(company, param_name)
#
#                     if company_value is None:
#                         all_passed = False
#                         break
#
#                     if compare_values(company_value, operator, threshold):
#                         reasons.append(f"{param_name}: {company_value:.2f} {operator} {threshold:.2f} ✅")
#                     else:
#                         all_passed = False
#                         break
#
#                 if all_passed:
#                     reason_text = " | ".join(reasons)
#                     passed_companies.append({
#                         "company_name": company_name,
#                         "sector": sector,
#                         "status": "PASS",
#                         "reason": reason_text,
#                         "company_details": company
#                     })
#
#             except Exception:
#                 continue
#
#         return passed_companies
#     except Exception:
#         return []
#
#
# def compare_values(actual: float, operator: str, threshold: float) -> bool:
#     """Compare actual vs threshold"""
#     try:
#         if actual is None or threshold is None:
#             return False
#
#         if operator == ">" and threshold == 0:
#             return actual > 0
#         elif operator == ">":
#             return actual > threshold
#         elif operator == ">=":
#             return actual >= threshold
#         elif operator == "<":
#             return actual < threshold
#         elif operator == "<=":
#             return actual <= threshold
#         elif operator == "==":
#             return actual == threshold
#         return False
#     except Exception:
#         return False
#
# # ============================================================================
# # TOOL 1: SCALE & LIQUIDITY SCREENING
# # ============================================================================
#
# class ScaleLiquidityScreeningTool(BaseTool):
#     """
#     Screens companies against SCALE & LIQUIDITY criteria:
#     - Revenue
#     - EBITDA
#     - Net Income
#     - Market Cap
#     """
#     name: str = "scale_liquidity_screening_tool"
#     description: str = """Screen companies against scale & liquidity mandate parameters.
#     Checks: Revenue, EBITDA, Net Income, Market Cap.
#     Returns companies that pass these specific criteria."""
#
#     def _run(self, mandate_parameters: dict, companies: list) -> str:
#         """
#         Filter mandate_parameters to only SCALE/LIQUIDITY metrics,
#         then screen companies against those.
#         """
#         try:
#             # Extract only scale/liquidity parameters from mandate
#             scale_liquidity_params = {
#                 k: v for k, v in mandate_parameters.items()
#                 if k.lower() in ["revenue", "ebitda", "net_income", "market_cap"]
#             }
#
#             # If no scale/liquidity params, return empty
#             if not scale_liquidity_params:
#                 print(f"[Scale/Liquidity Tool] No scale/liquidity params in mandate. Skipping.")
#                 return json.dumps({"company_details": [], "tool_used": "scale_liquidity"})
#
#             print(f"\n[SCALE/LIQUIDITY TOOL] Screening {len(companies)} companies")
#             print(f"  Parameters: {list(scale_liquidity_params.keys())}")
#
#             # Screen against scale/liquidity only
#             passed_companies = screen_companies_simple(scale_liquidity_params, companies)
#
#             print(f"  Result: {len(passed_companies)} passed scale/liquidity criteria")
#
#             # Format response
#             company_details_list = []
#             for company in passed_companies:
#                 company_data = company["company_details"].copy()
#                 company_data["status"] = "scale_liquidity_pass"
#                 company_details_list.append(company_data)
#
#             return json.dumps({
#                 "company_details": company_details_list,
#                 "tool_used": "scale_liquidity",
#                 "count": len(company_details_list)
#             }, default=str)
#
#         except Exception as e:
#             print(f"[Scale/Liquidity Tool] Error: {str(e)}")
#             import traceback
#             traceback.print_exc()
#             return json.dumps({"company_details": [], "tool_used": "scale_liquidity", "error": str(e)})
#
#
# # ============================================================================
# # TOOL 2: PROFITABILITY & VALUATION SCREENING
# # ============================================================================
#
# class ProfitabilityValuationScreeningTool(BaseTool):
#     """
#     Screens companies against PROFITABILITY & VALUATION criteria:
#     - Gross Profit Margin
#     - Growth
#     - Return on Equity
#     - Debt / Equity
#     - P/E Ratio
#     - Price / Book
#     - Dividend Yield
#     """
#     name: str = "profitability_valuation_screening_tool"
#     description: str = """Screen companies against profitability & valuation mandate parameters.
#     Checks: Gross Profit Margin, ROE, Debt/Equity, P/E, Price/Book, Dividend Yield.
#     Returns companies that pass these specific criteria."""
#
#     def _run(self, mandate_parameters: dict, companies: list) -> str:
#         """
#         Filter mandate_parameters to only PROFITABILITY/VALUATION metrics,
#         then screen companies against those.
#         """
#         try:
#             # Extract only profitability/valuation parameters from mandate
#             prof_val_params = {
#                 k: v for k, v in mandate_parameters.items()
#                 if k.lower() in [
#                     "gross_profit_margin", "return_on_equity", "debt_to_equity",
#                     "pe_ratio", "price_to_book", "dividend_yield","growth"
#                 ]
#             }
#
#             # If no profitability/valuation params, return empty
#             if not prof_val_params:
#                 print(f"[Profitability/Valuation Tool] No prof/val params in mandate. Skipping.")
#                 return json.dumps({"company_details": [], "tool_used": "profitability_valuation"})
#
#             print(f"\n[PROFITABILITY/VALUATION TOOL] Screening {len(companies)} companies")
#             print(f"  Parameters: {list(prof_val_params.keys())}")
#
#             # Screen against profitability/valuation only
#             passed_companies = screen_companies_simple(prof_val_params, companies)
#
#             print(f"  Result: {len(passed_companies)} passed profitability/valuation criteria")
#
#             # Format response
#             company_details_list = []
#             for company in passed_companies:
#                 company_data = company["company_details"].copy()
#                 company_data["status"] = "prof_val_pass"
#                 company_details_list.append(company_data)
#
#             return json.dumps({
#                 "company_details": company_details_list,
#                 "tool_used": "profitability_valuation",
#                 "count": len(company_details_list)
#             }, default=str)
#
#         except Exception as e:
#             print(f"[Profitability/Valuation Tool] Error: {str(e)}")
#             import traceback
#             traceback.print_exc()
#             return json.dumps({"company_details": [], "tool_used": "profitability_valuation", "error": str(e)})
#
#
#
#
# # ============================================================================
# # CUSTOM TOOL: Financial Screening - NO REASONING
# # ============================================================================
#
# class FinancialScreeningTool(BaseTool):
#     """Validates companies against mandate parameters - returns ONLY passed companies"""
#     name: str = "financial_screening_tool"
#     description: str = """Screen companies against mandate parameters and return only those that pass ALL criteria.
#     Tool returns ONLY the filtered results - Agent will provide analysis and reasoning."""
#
#     def _run(self, mandate_parameters: dict, companies: list) -> str:
#         """Screen companies and return passed ones WITHOUT reasoning"""
#         try:
#             print(f"\nTool Screening {len(companies)} companies against {len(mandate_parameters)} criteria...")
#
#             if not mandate_parameters or not companies:
#                 return json.dumps({"company_details": []})
#
#             passed_companies = screen_companies_simple(mandate_parameters, companies)
#             print(f"Tool Result: {len(passed_companies)} companies passed")
#
#             company_details_list = []
#             for company in passed_companies:
#                 company_data = company["company_details"].copy()
#                 company_data["status"] = "Pass"
#                 company_details_list.append(company_data)
#
#             formatted_response = {"company_details": company_details_list}
#             print(f"Tool Output: {len(company_details_list)} qualified companies")
#             return json.dumps(formatted_response, default=str)
#
#         except Exception as e:
#             print(f"Tool Error: {str(e)}")
#             import traceback
#             traceback.print_exc()
#             return json.dumps({"company_details": []})
#
#
# # ============================================================================
# # CREWAI AGENT
# # ============================================================================
#
# try:
#
#     azure_llm = LLM(
#         model=llm_config["model"],
#         api_key=llm_config["api_key"],
#         base_url=llm_config["api_base"],
#         api_version=llm_config["api_version"],
#         temperature=llm_config.get("temperature", 0.3),
#         max_tokens=llm_config.get("max_tokens", 2048)
#     )
#
#     financial_screening_agent = Agent(
#         role="Financial Screening Specialist",
#         goal="""This agent executes the 'Bottom-Up Fundamental Analysis' sub-process as part of the 'Research and Idea Generation process' under the Fund Mandate capability. It focuses on 'granular', 'company-specific evaluation', 'including financial statement analysis', 'earnings modeling', and 'intrinsic valuation'. Use this agent for deep dives into individual securities to determine if they meet the specific criteria of the investment mandate.
#         Evaluate companies against fund mandate parameters.
#         choose and Use relevant tool to validate each company.
#         Return ONLY valid JSON output with screening results.""",
#         backstory="""You are an expert financial analyst specializing in investment screening.
#         You have deep knowledge of financial metrics, valuation multiples, and
#         institutional investment criteria. You evaluate companies objectively
#         against predefined mandate parameters.""",
#         llm=azure_llm,
#         tools=[
#         ScaleLiquidityScreeningTool(),
#         ProfitabilityValuationScreeningTool()
#         ],
#         verbose=True,
#         reasoning=True,
#         allow_delegation=False
#     )
# except Exception as e:
#     print(f"Agent initialization error: {e}")
#     financial_screening_agent = None
#
# # ============================================================================
# # CREWAI TASK
# # ============================================================================
#
# try:
#     screen_companies_task = Task(
#         description="""
#         This agent executes the Bottom-Up Fundamental Analysis sub-process as part of the Research and Idea Generation process under the Fund Mandate capability. It focuses on granular, company-specific evaluation, including financial statement analysis, earnings modeling, and intrinsic valuation. Use this agent for deep dives into individual securities to determine if they meet the specific criteria of the investment mandate.
#         Screen companies against fund mandate parameters.
#
#         Mandate Parameters:
#         {mandate_parameters}
#
#         Companies to Screen:
#         {companies}
#
#         TASK:
#         1. 1. Analyze mandate parameters to determine which type(s) apply and Choose tools intelligently by reading teh tools description:
#    - Companies must pass ALL non-null criteria from both tools
#    - Generate unified reason field combining all passed criteria
#
#
#         2.Before calling the tool, build a **financial-only view** of each company called `financial_only_companies`:
#         - Keep only the financial fields the tool can parse:
#         - Identification: "Company" (or "Company "), "Ticker" (if present)
#         - Financials the tool supports:
#             "Revenue"
#             "Dividend Yield"
#             "5-Years Growth"
#             "Net Income"
#             "Total Assets"
#             "Total Equity"
#             "EPS / Forecast"
#             "EBITDA"
#             "1-Year Change"
#             "P/E Ratio"
#             "Debt / Equity"
#             "Price/Book"
#             "Return on Equity"
#             "Market Cap"
#             "Gross Profit Margin"
#         - Remove all other fields (risk, geography, ESG, portfolio data, etc.).
#         - Then call `financial_screening_tool` with:
#         { "mandate_parameters": mandate_parameters, "companies": financial_only_companies }
#         2. Tool returns JSON with ONLY passed companies
#         3. Extract results and return as JSON
#         4.provide the reason for the particular company why it has passed the screening dynamically according to the threshold comparison against the mandate_parameters.add in the output json in the reason key.
#         5.Build `conditionally_qualified`:
#         - Consider companies that are **not** in the PASSED set.
#         - If a company would meet **all required thresholds** except for one or more **null/None/empty** required metrics, include it here with `"status": "Conditional"`.
#         - The `reason` must clearly say: which metrics are null/missing and that other required metrics meet thresholds (e.g., "EBITDA is null; all other required metrics meet the mandate").
#         - Do **not** include companies here if they fail any non-null metric threshold.
#
#
#         """,
#         expected_output="""Valid JSON array with ONLY passed companies:
#         {
#             "company_details": [
#                 {
#                     "id": "unique_company_id",
#                     "Company": "company_name",
#                     "Country": "country",
#                     "Sector": "sector",
#                     "status": "Pass",
#                     "reason": " .... ",
#                     "Revenue": "revenue_value",
#                     "Dividend Yield": "dividend_yield_value",
#                     "5-Years Growth": growth_value,
#                     ... all financial metrics  given as input all original including risk metrics
#                 },
#
#                 {
#                     "id": "unique_company_id",
#                     "Company": "company_name",
#                     "Country": "country",
#                     "Sector": "sector",
#                     "status": "Conditional",
#                     "reason": "",
#                     "Revenue": "revenue_value",
#                     "Dividend Yield": "dividend_yield_value",
#                     "5-Years Growth": growth_value,
#                     ... all financial metrics  given as input all original including risk metrics
#                 }
#             ]
#         }",""",
#         agent=financial_screening_agent
#     )
# except Exception as e:
#     print(f"Task initialization error: {e}")
#     screen_companies_task = None
#
# # ============================================================================
# # CREWAI CREW
# # ============================================================================
#
# try:
#     screening_crew = Crew(
#         agents=[financial_screening_agent],
#         tasks=[screen_companies_task],
#         process=Process.sequential,
#         verbose=True
#     )
# except Exception as e:
#     print(f"Crew initialization error: {e}")
#     screening_crew = None
#
#
# # ============================================================================
# # RIGID JSON PARSING - HANDLES BACKTICKS & MARKDOWN
# # ============================================================================
#
# def extract_and_parse_json(result_text: str) -> dict:
#     """
#     RIGID JSON PARSING - Handles all formats including backticks
#     """
#     print(f"\n RIGID JSON PARSING STARTED")
#     print(f"Result length: {len(result_text)} chars\n")
#
#     # Strategy 1: Remove markdown backticks first
#     print("Strategy 1: Removing markdown backticks...")
#     cleaned_text = result_text.strip()
#
#     # Remove ``` json ... ``` wrappers
#     if cleaned_text.startswith('```'):
#         cleaned_text = re.sub(r'^```(?:json)?\s*', '', cleaned_text)
#         cleaned_text = re.sub(r'```\s*$', '', cleaned_text)
#         cleaned_text = cleaned_text.strip()
#         print("✓ Removed markdown backticks")
#
#     # Strategy 2: Direct JSON parse
#     print("Strategy 2: Direct JSON parse...")
#     try:
#         raw_parsed = json.loads(cleaned_text)
#         if "company_details" in raw_parsed and isinstance(raw_parsed.get("company_details"), list):
#             print(f"SUCCESS: Direct JSON parse - {len(raw_parsed['company_details'])} companies\n")
#             return raw_parsed
#     except json.JSONDecodeError as e:
#         print(f"Direct JSON failed: {e}\n")
#
#     # Strategy 3: Extract JSON between braces
#     print("Strategy 3: Extracting JSON between braces...")
#     start = cleaned_text.find('{')
#     end = cleaned_text.rfind('}') + 1
#
#     if start != -1 and end > start:
#         json_str = cleaned_text[start:end]
#         try:
#             raw_parsed = json.loads(json_str)
#             if "company_details" in raw_parsed and isinstance(raw_parsed.get("company_details"), list):
#                 print(f"SUCCESS: Brace extraction - {len(raw_parsed['company_details'])} companies\n")
#                 return raw_parsed
#         except json.JSONDecodeError as e:
#             print(f"Brace extraction failed: {e}\n")
#
#     # Strategy 4: Look for JSON array
#     print("Strategy 4: Extracting JSON array...")
#     json_array_match = re.search(r'\[\s*\{.*?\}\s*\]', cleaned_text, re.DOTALL)
#     if json_array_match:
#         try:
#             json_str = json_array_match.group(0)
#             companies_array = json.loads(json_str)
#             if isinstance(companies_array, list) and len(companies_array) > 0:
#                 print(f"SUCCESS: Array extraction - {len(companies_array)} companies\n")
#                 return {"company_details": companies_array}
#         except json.JSONDecodeError as e:
#             print(f"Array extraction failed: {e}\n")
#
#     # Strategy 5: Remove common problematic characters
#     print("Strategy 5: Cleaning problematic characters...")
#     cleaned_text = cleaned_text.replace('\n', ' ').replace('\\', '')
#
#     start = cleaned_text.find('{')
#     end = cleaned_text.rfind('}') + 1
#
#     if start != -1 and end > start:
#         json_str = cleaned_text[start:end]
#         try:
#             raw_parsed = json.loads(json_str)
#             if "company_details" in raw_parsed:
#                 print(f"SUCCESS: Cleaned extraction - {len(raw_parsed['company_details'])} companies\n")
#                 return raw_parsed
#         except json.JSONDecodeError as e:
#             print(f"Cleaned extraction failed: {e}\n")
#
#     print("All parsing strategies failed\n")
#     return {"company_details": []}
#
#
# # ============================================================================
# # UPDATED WEBSOCKET SCREENING FUNCTION
# # ============================================================================
#
# async def run_screening_with_websocket(
#         websocket: WebSocket,
#         mandate_id: str,
#         mandate_parameters: dict,
#         companies: list
# ) -> dict:
#     """Run screening with REAL-TIME streaming"""
#
#     try:
#         callback = WebSocketStreamingCallback(websocket)
#         await callback.on_agent_initialized()
#         await asyncio.sleep(0.5)
#
#         if not screening_crew:
#             await callback.on_error("Screening crew not initialized")
#             return {"company_details": []}
#
#         current_loop = asyncio.get_event_loop()
#         original_stdout = sys.stdout
#         event_capture = RealtimeEventCapture(original_stdout, callback, current_loop)
#         sys.stdout = event_capture
#
#         try:
#             print("Executing crew with real-time event streaming...\n")
#
#             # Convert mandate_id to string to ensure consistency
#             mandate_id_str = str(mandate_id)
#
#             print(f" Mandate ID being passed: {mandate_id_str}")
#
#             result = await asyncio.to_thread(
#                 screening_crew.kickoff,
#                 inputs={
#                     "mandate_id": mandate_id_str,  # ← Ensure it's a string
#                     "mandate_parameters": mandate_parameters,
#                     "companies": companies
#                 }
#             )
#
#             print("\nCrew execution complete!")
#             print("\nToken Usage: " + str(screening_crew.usage_metrics))
#
#         finally:
#             sys.stdout = original_stdout
#
#         await asyncio.sleep(1.5)
#
#         await callback.on_screening_progress(
#             f"Total companies evaluated: {len(companies)}\n"
#             f"Screening criteria applied: {len(mandate_parameters)}"
#         )
#
#         raw_text = result.raw.strip() if hasattr(result, "raw") else str(result).strip()
#         parsed_result = extract_and_parse_json(raw_text)
#
#         # FORCE CORRECT MANDATE ID - with logging
#         original_mandate_id = parsed_result.get("mandate_id")
#         parsed_result["mandate_id"] = mandate_id_str
#
#         if original_mandate_id != mandate_id_str:
#             print(f" MANDATE ID CORRECTED:")
#             print(f"   Expected: {mandate_id_str}")
#             print(f"   Got from agent: {original_mandate_id}")
#             print(f" Forced to: {mandate_id_str}")
#
#         num_qualified = len(parsed_result.get("company_details", []))
#
#         await asyncio.sleep(0.5)
#         await callback.on_agent_finish(
#             f"Screening analysis complete.\nCompanies qualified: {num_qualified}"
#         )
#
#         await asyncio.sleep(0.5)
#
#         final_json = json.dumps(parsed_result, indent=2, default=str)
#         await callback.on_final_output((final_json or "")[:1000])
#
#         return parsed_result
#
#     except Exception as e:
#         print(f"Error: {e}")
#         import traceback
#         traceback.print_exc()
#
#         await callback.on_error(str(e))
#         return {"company_details": []}
#


# llm changed gpt-5(new) ; token sending in websocket ;tool taking input from the db ; tool giving company_id & passed as well as null valued companies.


import os
import json
import re
import asyncio
import sys
from io import StringIO
from typing import Optional, List, Any, Dict, Iterable
from collections import defaultdict
from crewai import Agent, Task, Crew, Process, LLM
from crewai.tools import BaseTool
from dotenv import load_dotenv
from fastapi import WebSocket
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import litellm
from database.models import Sourcing, Company
from database.repositories.screeningRepository import ScreeningRepository

load_dotenv()

KEY_VAULT_NAME = "fstodevazureopenai"
KEY_VAULT_URL = f"https://{KEY_VAULT_NAME}.vault.azure.net/"

SECRETS_MAP = {
    "api_key": "llm-api-key",
    "endpoint": "llm-base-endpoint",
    "deployment": "llm-41",
    "api_version": "llm-41-version"
}

# Global LLM config
llm_config = None


def get_secrets_from_key_vault():
    """Retrieve LLM secrets from Azure Key Vault"""
    try:

        credential = DefaultAzureCredential()
        kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

        secrets = {}
        for key, secret_name in SECRETS_MAP.items():
            try:
                secret_value = kv_client.get_secret(secret_name).value
                secrets[key] = secret_value

            except Exception as e:
                print(f"  ❌ Failed to retrieve '{secret_name}': {e}")
                raise

        return secrets

    except Exception as e:
        raise


def initialize_azure_llm_config():
    """Initialize Azure OpenAI LLM config for CrewAI"""
    global llm_config

    try:

        # Get secrets from KeyVault
        secrets = get_secrets_from_key_vault()

        # Set environment variables for litellm/Azure
        os.environ["AZURE_API_KEY"] = secrets['api_key']
        os.environ["AZURE_API_BASE"] = secrets['endpoint']
        os.environ["AZURE_API_VERSION"] = secrets['api_version']

        llm_config = {
            "model": f"azure/{secrets['deployment']}",
            "api_key": secrets['api_key'],
            "api_base": secrets['endpoint'],
            "api_version": secrets['api_version'],
            "temperature": 0.3,
            "max_tokens": 16384
        }

        return llm_config

    except Exception as e:

        raise


try:
    llm_config = initialize_azure_llm_config()
except Exception as e:

    raise


# ============================================================================
# UPDATED: REAL-TIME EVENT CAPTURE FOR MULTIPLE TOOLS - WITH MULTIPLE AGENT THINKING
# ============================================================================

class RealtimeEventCapture:
    """Capture events in real-time from stdout - HANDLES MULTIPLE TOOL EXECUTIONS WITH SEPARATE THINKING"""

    def __init__(self, original_stdout, callback, loop):
        self.original_stdout = original_stdout
        self.callback = callback
        self.loop = loop
        self.buffer = ""

        # Track events
        self.reasoning_sent = False
        self.thought_sent = False
        self.tools_executed = []
        self.last_tool_thinking = {}  # Track thinking for each tool

    def write(self, text: str) -> None:
        """Write to terminal AND check for events"""
        self.original_stdout.write(text)
        self.original_stdout.flush()
        self.buffer += text
        self._check_events_in_order()

    def _clean_text(self, text: str) -> str:
        """Remove non-ASCII characters and special Unicode"""
        cleaned = text.replace('\xa0', ' ')
        cleaned = cleaned.replace('\n', ' ')
        cleaned = cleaned.replace('\r', ' ')
        cleaned = re.sub(r'[^\x20-\x7E]', '', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned.strip()

    def _check_events_in_order(self) -> None:
        """Check for events - handles multiple tool executions with separate thinking"""
        try:
            # EVENT 1: Reasoning Plan
            if ("Reasoning Plan" in self.buffer and not self.reasoning_sent):
                reasoning_match = re.search(
                    r'Reasoning Plan(.*?)(?=Agent:|$)',
                    self.buffer,
                    re.DOTALL
                )

                if reasoning_match:
                    reasoning_text = "Reasoning Plan" + reasoning_match.group(1)
                    reasoning_text = self._clean_text(reasoning_text)
                    if len(reasoning_text) >= 20:
                        self._send_event_safe(self.callback.on_reasoning_plan(reasoning_text))
                        self.reasoning_sent = True

            # EVENT 2 & MULTIPLE: Agent Thinking - CAPTURE FOR EACH TOOL
            flags = re.IGNORECASE
            if self.reasoning_sent:
                # Find ALL "Using Tool:" occurrences
                tool_patterns = list(re.finditer(r'Using\s*Tool:?\s*(\S+)', self.buffer, flags))

                for idx, tool_match in enumerate(tool_patterns):
                    tool_name = tool_match.group(1).strip()

                    # Skip if we already processed thinking for this tool
                    if tool_name in self.last_tool_thinking:
                        continue

                    # Look backwards from tool position to find preceding Thought/Agent
                    tool_pos = tool_match.start()
                    preceding_buffer = self.buffer[:tool_pos]

                    # Find the LAST Agent, Thought, and Action before this tool
                    agent_match = None
                    thought_match = None
                    action_match = None

                    # Search backwards from tool position
                    for prev_agent in re.finditer(r'Agent:\s*([^\n]+)', preceding_buffer, flags):
                        agent_match = prev_agent
                    for prev_thought in re.finditer(r'Thought:\s*([^\n]+)', preceding_buffer, flags):
                        thought_match = prev_thought
                    for prev_action in re.finditer(r'Action:\s*([^\n]+)', preceding_buffer, flags):
                        action_match = prev_action

                    # Only send if we have agent and thought
                    if agent_match and thought_match:
                        agent = self._clean_text(agent_match.group(1))
                        thought = self._clean_text(thought_match.group(1))
                        action = self._clean_text(action_match.group(1)) if action_match else None

                        parts = [f"Agent: {agent}", "", f"Thought: {thought}"]
                        if action:
                            parts += ["", f"Action: {action}"]
                        parts += ["", f"Using Tool: {tool_name}"]

                        thinking_msg = "\n".join(parts)
                        self._send_event_safe(self.callback.on_agent_thinking(thinking_msg))
                        self.last_tool_thinking[tool_name] = True

                        self.original_stdout.write(
                            f"\n✓ [EVENT 2.{len(self.last_tool_thinking)}] Agent thinking sent for {tool_name}\n")
                        self.original_stdout.flush()

            # EVENT 3 & 4: MULTIPLE TOOL EXECUTIONS
            if self.reasoning_sent:
                tool_patterns = re.finditer(r'Using Tool:\s*(\S+)', self.buffer, flags)

                for match in tool_patterns:
                    tool_name = match.group(1).strip()

                    # Skip if we already sent event for this tool
                    if tool_name in self.tools_executed:
                        continue

                    # Send TOOL START event
                    self._send_event_safe(self.callback.on_tool_start(tool_name))
                    self.original_stdout.write(f"\n✓ [EVENT 3] Tool start sent: {tool_name}\n")
                    self.original_stdout.flush()

                    self.tools_executed.append(tool_name)

                # Check for tool completion
                for tool_name in self.tools_executed:
                    if (f"[{tool_name}]" in self.buffer or
                            "Tool Result:" in self.buffer or
                            "companies passed" in self.buffer.lower()):
                        result_match = re.search(
                            r'(\d+)\s*(?:companies?|passed)',
                            self.buffer,
                            re.IGNORECASE
                        )
                        count = result_match.group(1) if result_match else "companies"

                        self._send_event_safe(self.callback.on_tool_end(
                            tool_name,
                            f"{count} passed {tool_name} criteria"
                        ))
                        self.original_stdout.write(f"\n✓ [EVENT 4] Tool end sent: {tool_name}\n")
                        self.original_stdout.flush()

        except Exception as e:
            self.original_stdout.write(f"\n⚠️ Event capture error: {e}\n")
            self.original_stdout.flush()

    def _send_event_safe(self, coro):
        """Safely send coroutine to event loop from thread"""
        try:
            if self.loop and self.loop.is_running():
                asyncio.run_coroutine_threadsafe(coro, self.loop)
        except Exception:
            pass

    def flush(self) -> None:
        self.original_stdout.flush()

    def get_buffer(self) -> str:
        return self.buffer


class WebSocketStreamingCallback:
    """Stream events to WebSocket with content cleaning"""

    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.step_count = 0
        self.tools_started = 0  # Track tool execution count

    def _clean_content(self, content: str) -> str:
        """Remove non-ASCII, ANSI codes, and special Unicode characters"""
        ansi_escape_pattern = r'\x1b\[[0-9;]*m|\[0m|\[32m|\[37m'
        cleaned = re.sub(ansi_escape_pattern, '', content)

        cleaned = cleaned.replace('\xa0', ' ')
        cleaned = cleaned.replace('\u200b', '')
        cleaned = cleaned.replace('\r', '')
        cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', cleaned)
        cleaned = re.sub(r'[^\x20-\x7E\n]', '', cleaned)
        cleaned = re.sub(r'  +', ' ', cleaned)
        cleaned = re.sub(r'\n\n+', '\n', cleaned)

        return cleaned.strip()

    async def send_event(self, event_type: str, content: str) -> None:
        """Send event to WebSocket"""
        try:
            self.step_count += 1
            cleaned_content = self._clean_content(content)

            message = {
                "type": event_type,
                "content": cleaned_content,
                "step": self.step_count
            }
            print(f"\n[STEP {self.step_count}] Sending: {event_type}")
            await self.websocket.send_json(message)
            await asyncio.sleep(0.3)
        except Exception as e:
            print(f"WebSocket error: {e}")

    async def on_agent_initialized(self) -> None:
        content = """STEP 1: Agent Initialized

Bottom-Up Fundamental Analysis agent is ready
Initializing screening process..."""
        await self.send_event("step_1", content)

    async def on_reasoning_plan(self, plan: str) -> None:
        content = f"""STEP 2A: Reasoning Plan

{plan}"""
        await self.send_event("step_2a", content)

    async def on_agent_thinking(self, thought: str) -> None:
        content = f"""STEP 2B: Bottom-Up Fundamental Analysis Agent Thinking

{thought}"""
        await self.send_event("step_2b", content)

    async def on_tool_start(self, tool_name: str) -> None:
        """Send tool start event with SPECIFIC TOOL NAME"""
        self.tools_started += 1
        # Format tool name nicely
        tool_display = tool_name.replace("_", " ").title()

        content = f"""STEP 3.{self.tools_started}: Tool Execution Started

Executing: {tool_display}
Screening companies against mandate parameters..."""
        await self.send_event(f"step_3_{self.tools_started}", content)

    async def on_tool_end(self, tool_name: str, output: str) -> None:
        """Send tool end event with SPECIFIC TOOL NAME and results"""
        tool_display = tool_name.replace("_", " ").title()

        content = f"""STEP 4.{self.tools_started}: Tool Completed

{tool_display} executed successfully
Result: {output}"""
        await self.send_event(f"step_4_{self.tools_started}", content)

    async def on_screening_progress(self, message: str) -> None:
        content = f"""STEP 5: Results Processing

{message}"""
        await self.send_event("step_5", content)

    async def on_agent_finish(self, result: str) -> None:
        content = f"""STEP 6: Bottom-Up Fundamental Analysis Agent Task Completed

{result}"""
        await self.send_event("step_6", content)

    async def on_final_output(self, output: str) -> None:
        content = f"""STEP 7: Final Output Ready

{output}"""
        await self.send_event("step_7", content)

    async def on_error(self, error: str) -> None:
        await self.send_event("error", f"Error: {error}")


# ============================================================================
# HELPER FUNCTIONS FOR SCREENING (CLEANED - NO DEBUG PRINTS)
# ============================================================================

def parse_constraint(constraint_str: str) -> tuple:
    """Parse constraint - handles $, %, B, M, T and converts all thresholds into MILLIONS."""
    try:
        constraint_str = str(constraint_str).strip()
        constraint_str = constraint_str.replace("&amp;gt;", "&gt;").replace("&amp;lt;", "&lt;")

        # Special cases
        if constraint_str.lower() == "positive":
            return ">", 0

        if constraint_str.lower() in ["", "-", "na", "n/a", "none", "null"]:
            return "skip", 0

        if "not required" in constraint_str.lower():
            return "skip", 0

        constraint_str = constraint_str.replace("&amp;amp;gt;", "&gt;").replace("&amp;amp;lt;", "&lt;")

        # Extract operator and number
        match = re.search(r'([><]=?|==|!=)\s*([\d.]+)', constraint_str)
        if not match:
            return ">", 0

        operator = match.group(1)
        threshold = float(match.group(2))

        # Identify units
        has_dollar = '$' in constraint_str
        has_billion = 'B' in constraint_str.upper() and 'M' not in constraint_str.upper()
        has_trillion = 'T' in constraint_str.upper()
        has_million = 'M' in constraint_str.upper()
        has_percent = '%' in constraint_str

        # Convert currency amounts → millions
        if has_dollar:
            if has_billion:
                threshold = threshold * 1000
            elif has_trillion:
                threshold = threshold * 1000000
            elif not has_million:
                threshold = threshold / 1_000_000

        # Convert % → decimal
        if has_percent:
            if threshold > 1:
                threshold = threshold / 100

        return operator, threshold

    except Exception as e:
        return ">", 0


def get_company_value(company: dict, param_name: str) -> Optional[float]:
    """Get numeric value from company - ALL VALUES IN MILLIONS"""
    try:
        param_lower = param_name.lower()

        # Handle REVENUE
        if param_lower == "revenue":
            revenue = company.get("Revenue")
            if revenue is None:
                return None
            return parse_value(revenue)

        # Handle NET INCOME
        if param_lower == "net_income":
            net_income = company.get("Net Income")
            if net_income is None:
                return None
            return parse_value(net_income)

        # Handle MARKET CAP
        if param_lower == "market_cap":
            market_cap = company.get("Market Cap")
            if market_cap is None:
                return None
            return parse_value(market_cap)

        # Handle EBITDA
        if param_lower == "ebitda":
            ebitda_raw = company.get("EBITDA")
            if ebitda_raw is None:
                return None
            return parse_value(ebitda_raw)

        # Handle GROSS PROFIT MARGIN
        if param_lower == "gross_profit_margin":
            gpm = company.get("Gross Profit Margin")
            if gpm is None:
                return None
            parsed = parse_value(gpm)
            if parsed is None:
                return None
            if parsed > 1:
                parsed = parsed / 100
            return parsed

        # Handle RETURN ON EQUITY
        if param_lower == "return_on_equity":
            roe = company.get("Return on Equity")
            if roe is None:
                return None
            parsed = parse_value(roe)
            if parsed is None:
                return None
            if parsed > 1:
                parsed = parsed / 100
            return parsed

        # Standard field mapping
        field_map = {
            "debt_to_equity": ["Debt / Equity"],
            "pe_ratio": ["P/E Ratio"],
            "price_to_book": ["Price/Book"],
            "dividend_yield": ["Dividend Yield"]
        }

        fields = field_map.get(param_lower, [param_name])

        for field in fields:
            if field in company:
                value = company[field]
                if value is None:
                    continue
                parsed = parse_value(value)
                if parsed is not None:
                    return parsed

        return None
    except Exception:
        return None


def parse_value(value: Any) -> Optional[float]:
    """Parse various value formats (B, M, T, %) - RETURNS VALUE IN MILLIONS"""
    try:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            value_str = str(value).strip()

            # Remove newlines, %, $, commas
            value_str = value_str.replace("\n", "").replace("%", "").replace("$", "").replace(",", "")
            # Remove dots before B, M, T (e.g., "244.12B" -> "24412B")
            value_str = re.sub(r'(\d)\.(\d+)([BMT])', r'\1\2\3', value_str)

            # Handle T (trillions) -> convert to millions
            if 'T' in value_str.upper():
                value_str = value_str.upper().replace('T', '').strip()
                return float(value_str) * 1000000

            # Handle B (billions) -> convert to millions
            if 'B' in value_str.upper():
                value_str = value_str.upper().replace('B', '').strip()
                return float(value_str) * 1000

            # Handle M (millions)
            if 'M' in value_str.upper():
                value_str = value_str.upper().replace('M', '').strip()
                return float(value_str)

            # Plain number
            if value_str:
                return float(value_str)

        return None
    except Exception:
        return None


# ============================================================================
# UPDATED SCREENING FUNCTION - PRESERVE COMPANY_ID
# ============================================================================

def screen_companies_simple(mandate_parameters: dict, companies: list) -> dict:
    """
    Screen companies against mandate parameters.
     OPTIMIZED: Preserves company_id from input without copying
    Returns dict with 'passed' and 'conditional' companies.
    """
    passed_companies = []
    conditional_companies = []

    try:
        if not mandate_parameters or not companies:
            return {"passed": [], "conditional": []}

        for company in companies:
            try:
                company_name = company.get("Company ", company.get("Company", "Unknown")).strip()
                sector = company.get("Sector", "Unknown").strip()
                company_id = company.get("company_id")  # Extract company_id from input

                all_passed = True
                all_non_null_passed = True
                null_params = []
                reasons = []

                for param_name, constraint_str in mandate_parameters.items():
                    if "not required" in str(constraint_str).lower():
                        continue

                    operator, threshold = parse_constraint(constraint_str)

                    if operator == "skip":
                        continue

                    company_value = get_company_value(company, param_name)

                    if company_value is None:
                        null_params.append(param_name)
                        all_passed = False
                        continue

                    if compare_values(company_value, operator, threshold):
                        reasons.append(f"{param_name}: {company_value:.2f} {operator} {threshold:.2f}")
                    else:
                        all_passed = False
                        all_non_null_passed = False
                        break

                # PASSED
                if all_passed:
                    reason_text = " | ".join(reasons)
                    passed_companies.append({
                        "company_name": company_name,
                        "sector": sector,
                        "company_id": company_id,  # Preserve company_id
                        "status": "Pass",
                        "reason": reason_text,
                        "company_details": company  # Keep original company dict with company_id
                    })

                # CONDITIONAL
                elif all_non_null_passed and null_params:
                    reason_text = " | ".join(reasons)
                    null_params_text = ", ".join(null_params)
                    full_reason = f"Missing data: {null_params_text}. All other required metrics meet the mandate: {reason_text}"

                    conditional_companies.append({
                        "company_name": company_name,
                        "sector": sector,
                        "company_id": company_id,  # Preserve company_id
                        "status": "Conditional",
                        "reason": full_reason,
                        "null_parameters": null_params,
                        "company_details": company  # Keep original company dict with company_id
                    })

            except Exception:
                continue

        return {"passed": passed_companies, "conditional": conditional_companies}

    except Exception:
        return {"passed": [], "conditional": []}


def compare_values(actual: float, operator: str, threshold: float) -> bool:
    """Compare actual vs threshold"""
    try:
        if actual is None or threshold is None:
            return False

        if operator == ">" and threshold == 0:
            return actual > 0
        elif operator == ">":
            return actual > threshold
        elif operator == ">=":
            return actual >= threshold
        elif operator == "<":
            return actual < threshold
        elif operator == "<=":
            return actual <= threshold
        elif operator == "==":
            return actual == threshold
        return False
    except Exception:
        return False


# ============================================================================
# OPTIMIZED: DATABASE HELPER - FETCH COMPANIES WITH COMPANY_ID
# ============================================================================
async def get_companies_by_mandate_id(mandate_id: int, company_id_list: List[int] = None) -> List[Dict[str, Any]]:
    """
    OPTIMIZED: Fetch companies from Sourcing table with company_id already embedded.
    Can filter by specific company IDs if provided.

    Args:
        mandate_id: Fund mandate ID
        company_id_list: Optional list of specific company IDs to fetch

    Returns:
        List of company_data dicts (each with company_id already in it).
    """
    try:
        # Build filter query
        filter_kwargs = {
            "fund_mandate_id": mandate_id,
            "deleted_at__isnull": True
        }

        # Query Sourcing table
        if company_id_list and len(company_id_list) > 0:
            # Filter by specific company IDs
            sourcing_records = await Sourcing.filter(**filter_kwargs).filter(company_id__in=company_id_list).all()
            print(
                f"\n [DB FETCH] Fetching specific companies for mandate_id={mandate_id}, company_ids={company_id_list}")
        else:
            # Fetch all companies for mandate
            sourcing_records = await Sourcing.filter(**filter_kwargs).all()
            print(f"\n [DB FETCH] Fetching ALL companies for mandate_id={mandate_id}")

        print(f"    Total records fetched from DB: {len(sourcing_records)}")

        companies_list = []

        for idx, sourcing in enumerate(sourcing_records, 1):
            company_id = sourcing.company_id
            company_data = sourcing.company_data

            # Handle different data formats
            if isinstance(company_data, str):
                try:
                    company_data = json.loads(company_data)
                except json.JSONDecodeError:
                    print(f"    Record {idx}: Failed to parse JSON, skipping")
                    continue
            elif isinstance(company_data, dict):
                pass
            else:
                print(f"    Record {idx}: Unknown data format, skipping")
                continue

            # Add company_id if not already present
            if "company_id" not in company_data:
                company_data["company_id"] = company_id

            company_name = company_data.get("Company", company_data.get("Company ", "Unknown"))
            print(f"    Record {idx}: ID={company_id}, Company={company_name}")

            companies_list.append(company_data)

        print(f"    Successfully processed {len(companies_list)} companies from DB\n")
        return companies_list

    except Exception as e:
        print(f" [DB FETCH] Error fetching companies by mandate_id: {e}")
        import traceback
        traceback.print_exc()
        return []


# ============================================================================
# OPTIMIZED TOOL 1: SCALE & LIQUIDITY SCREENING (CLEAN - NO LOGGING)
# ============================================================================

class ScaleLiquidityScreeningTool(BaseTool):
    """
    Screens companies against SCALE & LIQUIDITY criteria.
    OPTIMIZED: No extra copying, direct company_id preservation
    """
    name: str = "scale_liquidity_screening_tool"
    description: str = """Screen companies against scale & liquidity mandate parameters.
    Input: mandate_id, mandate_parameters, and optional company_id_list.
    Fetches companies from database for the mandate_id and filters by company_id_list if provided.
    Returns companies with company_id preserved."""

    def _run(self, mandate_id: int, mandate_parameters: dict, company_id_list: List[int] = None) -> str:
        """
        OPTIMIZED: Fetch once, screen once, return with company_id

        Args:
            mandate_id: Fund mandate ID
            mandate_parameters: Screening parameters
            company_id_list: Optional list of specific company IDs to screen
        """
        try:
            scale_liquidity_params = {
                k: v for k, v in mandate_parameters.items()
                if k.lower() in ["revenue", "ebitda", "net_income", "market_cap"]
            }

            if not scale_liquidity_params:
                print(f"[SCALE/LIQUIDITY TOOL] No scale/liquidity params in mandate. Skipping.")
                return json.dumps({"passed_companies": [], "conditional_companies": [], "tool_used": "scale_liquidity"})

            print(f"[SCALE/LIQUIDITY TOOL] Starting screening")
            print(f"  - Mandate ID: {mandate_id}")
            if company_id_list:
                print(f"  - Company IDs to screen: {company_id_list}")

            # Fetch companies (company_id already in each company dict)
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            companies_data = loop.run_until_complete(get_companies_by_mandate_id(mandate_id, company_id_list))

            if not companies_data:
                # print(f"[SCALE/LIQUIDITY TOOL] No companies found for mandate_id={mandate_id}")
                return json.dumps({"passed_companies": [], "conditional_companies": [], "tool_used": "scale_liquidity"})

            print(
                f"[SCALE/LIQUIDITY TOOL] Screening {len(companies_data)} companies against {len(scale_liquidity_params)} scale/liquidity parameters")

            # Screen companies (company_id is preserved in company_details)
            screening_results = screen_companies_simple(scale_liquidity_params, companies_data)
            passed_companies = screening_results.get("passed", [])
            conditional_companies = screening_results.get("conditional", [])

            print(f"[SCALE/LIQUIDITY TOOL] Results:")
            print(f"   Passed: {len(passed_companies)} companies")
            print(f"   Conditional: {len(conditional_companies)} companies")

            # Format output with company_id already present
            passed_list = []
            for company in passed_companies:
                company_data = company["company_details"]
                company_data["status"] = "Pass"
                company_data["reason"] = company.get("reason", "")
                passed_list.append(company_data)

            conditional_list = []
            for company in conditional_companies:
                company_data = company["company_details"]
                company_data["status"] = "Conditional"
                company_data["reason"] = company.get("reason", "")
                company_data["null_parameters"] = company.get("null_parameters", [])
                conditional_list.append(company_data)

            print(
                f"[SCALE/LIQUIDITY TOOL] Returning {len(passed_list)} passed + {len(conditional_list)} conditional companies\n")

            return json.dumps({
                "passed_companies": passed_list,
                "conditional_companies": conditional_list,
                "tool_used": "scale_liquidity",
                "passed_count": len(passed_list),
                "conditional_count": len(conditional_list)
            }, default=str)

        except Exception as e:
            print(f"[SCALE/LIQUIDITY TOOL] Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return json.dumps(
                {"passed_companies": [], "conditional_companies": [], "tool_used": "scale_liquidity", "error": str(e)})


# ============================================================================
# OPTIMIZED TOOL 2: PROFITABILITY & VALUATION SCREENING (WITH DEBUG LOGGING)
# ============================================================================

class ProfitabilityValuationScreeningTool(BaseTool):
    """
    Screens companies against PROFITABILITY & VALUATION criteria.
    OPTIMIZED: No extra copying, direct company_id preservation
    """
    name: str = "profitability_valuation_screening_tool"
    description: str = """Screen companies against profitability & valuation mandate parameters.
    Input: mandate_id, mandate_parameters, and optional company_id_list.
    Fetches companies from database for the mandate_id and filters by company_id_list if provided.
    Returns companies with company_id preserved."""

    def _run(self, mandate_id: int, mandate_parameters: dict, company_id_list: List[int] = None) -> str:
        """
         OPTIMIZED: Fetch once, screen once, return with company_id
        """
        try:
            prof_val_params = {
                k: v for k, v in mandate_parameters.items()
                if k.lower() in [
                    "gross_profit_margin", "return_on_equity", "debt_to_equity",
                    "pe_ratio", "price_to_book", "dividend_yield", "growth"
                ]
            }

            if not prof_val_params:
                print(f"[PROFITABILITY/VALUATION TOOL] No profitability/valuation params in mandate. Skipping.")
                return json.dumps(
                    {"passed_companies": [], "conditional_companies": [], "tool_used": "profitability_valuation"})

            print(f"[PROFITABILITY/VALUATION TOOL] Starting screening")
            print(f"  - Mandate ID: {mandate_id}")
            if company_id_list:
                print(f"  - Company IDs to screen: {company_id_list}")

            # Fetch companies (company_id already in each company dict)
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            companies_data = loop.run_until_complete(get_companies_by_mandate_id(mandate_id, company_id_list))

            if not companies_data:
                # print(f"[PROFITABILITY/VALUATION TOOL] No companies found for mandate_id={mandate_id}")
                return json.dumps(
                    {"passed_companies": [], "conditional_companies": [], "tool_used": "profitability_valuation"})

            print(
                f"[PROFITABILITY/VALUATION TOOL] Screening {len(companies_data)} companies against {len(prof_val_params)} profitability/valuation parameters")

            # Screen companies (company_id is preserved in company_details)
            screening_results = screen_companies_simple(prof_val_params, companies_data)
            passed_companies = screening_results.get("passed", [])
            conditional_companies = screening_results.get("conditional", [])

            print(f"[PROFITABILITY/VALUATION TOOL] Results:")
            print(f"  Passed: {len(passed_companies)} companies")
            print(f"   Conditional: {len(conditional_companies)} companies")

            # Format output with company_id already present
            passed_list = []
            for company in passed_companies:
                company_data = company["company_details"]
                company_data["status"] = "Pass"
                company_data["reason"] = company.get("reason", "")
                passed_list.append(company_data)

            conditional_list = []
            for company in conditional_companies:
                company_data = company["company_details"]
                company_data["status"] = "Conditional"
                company_data["reason"] = company.get("reason", "")
                company_data["null_parameters"] = company.get("null_parameters", [])
                conditional_list.append(company_data)

            print(
                f"[PROFITABILITY/VALUATION TOOL] Returning {len(passed_list)} passed + {len(conditional_list)} conditional companies\n")

            return json.dumps({
                "passed_companies": passed_list,
                "conditional_companies": conditional_list,
                "tool_used": "profitability_valuation",
                "passed_count": len(passed_list),
                "conditional_count": len(conditional_list)
            }, default=str)

        except Exception as e:
            print(f"[PROFITABILITY/VALUATION TOOL] Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return json.dumps(
                {"passed_companies": [], "conditional_companies": [], "tool_used": "profitability_valuation",
                 "error": str(e)})


# ============================================================================
# CREWAI AGENT
# ============================================================================
try:

    from crewai import LLM

    # Create LLM object with model configuration
    # llm = LLM(
    #         model=llm_config["model"],
    #         api_key=llm_config["api_key"],
    #         base_url=llm_config["base_url"],
    #         api_version=llm_config["api_version"],
    #         max_completion_tokens=2048,
    #         drop_params=True,  # Drop unsupported params automatically
    #         additional_drop_params=["stop", "temperature"],
    #         # Disable stop parameter for gpt-5-mini
    #         stop=None
    #     )

    azure_llm = LLM(
        model=llm_config["model"],
        api_key=llm_config["api_key"],
        base_url=llm_config["api_base"],
        api_version=llm_config["api_version"],
        temperature=llm_config.get("temperature", 0.3),
        max_tokens=llm_config.get("max_tokens", 2048)
    )

    financial_screening_agent = Agent(
        role="Financial Screening Specialist",
        goal="""This agent executes the 'Bottom-Up Fundamental Analysis' sub-process as part of the 'Research and Idea Generation process' under the Fund Mandate capability. It focuses on 'granular', 'company-specific evaluation', 'including financial statement analysis', 'earnings modeling', and 'intrinsic valuation'. Use this agent for deep dives into individual securities to determine if they meet the specific criteria of the investment mandate.
        Evaluate companies against fund mandate parameters.
        choose and Use relevant tool to validate each company.
        Return ONLY valid JSON output with screening results.""",
        backstory="""You are an expert financial analyst specializing in investment screening.
        You have deep knowledge of financial metrics, valuation multiples, and
        institutional investment criteria. You evaluate companies objectively
        against predefined mandate parameters. You understand that company_id from tool output
        must be preserved for database operations.""",
        llm=azure_llm,
        tools=[
            ScaleLiquidityScreeningTool(),
            ProfitabilityValuationScreeningTool()
        ],
        verbose=True,
        reasoning=True,
        allow_delegation=False,
        max_iter=2,
        cache=False,  # Limit iterations
    )
except Exception as e:
    print(f"Agent initialization error: {e}")
    financial_screening_agent = None

# ============================================================================
# CREWAI TASK
# ============================================================================

try:
    screen_companies_task = Task(
        description="""
        This agent executes the Bottom-Up Fundamental Analysis sub-process as part of the Research and Idea Generation process under the Fund Mandate capability. It focuses on granular, company-specific evaluation, including financial statement analysis, earnings modeling, and intrinsic valuation. Use this agent for deep dives into individual securities to determine if they meet the specific criteria of the investment mandate.
        Screen companies against fund mandate parameters.

       INPUTS:
        - Mandate ID: {mandate_id}
        - Mandate Parameters: {mandate_parameters}
        - Company ID List: {company_id_list} (optional - if provided, only screen these companies)


        TASK:
        1. 1. Analyze mandate parameters to determine which type(s) apply and Choose tools intelligently by reading teh tools description:
   - Companies must pass ALL non-null criteria from both tools
   - Generate unified reason field combining all passed criteria

        2. Tool returns JSON with ONLY passed companies
        3. Extract results and return as JSON.also use correct mandate_id that is being passed as input n the variable {mandate_id} in the result
        4.provide the reason for the particular company why it has passed the screening dynamically according to the threshold comparison against the mandate_parameters.add in the output json in the reason key.
        5.Build `conditionally_qualified`:
        - Consider companies that are **not** in the PASSED set.
        - If a company would meet **all required thresholds** except for one or more **null/None/empty** required metrics, include it here with `"status": "Conditional"`.
        - The `reason` must clearly say: which metrics are null/missing and that other required metrics meet thresholds (e.g., "EBITDA is null; all other required metrics meet the mandate").
        - Do **not** include companies here if they fail any non-null metric threshold.
        6. Return output with mandate_id = {mandate_id} (EXACTLY AS PROVIDED)


        """,
        expected_output="""Valid JSON array with ONLY passed companies:
        {
            "mandate_id": "{mandate_id}",
            "company_details": [
                {
                    "id": unique_integer_id,
                    "Company": "company_name",
                    "Country": "country",
                    "Sector": "sector",
                    "status": "Pass",
                    "reason": " .... ",
                    "Revenue": "revenue_value",
                    "Dividend Yield": "dividend_yield_value",
                    "5-Years Growth": growth_value,
                    ... all financial metrics  given as input all original including risk metrics
                },

                {
                    "id": unique_integer_id,
                    "Company": "company_name",
                    "Country": "country",
                    "Sector": "sector",
                    "status": "Conditional",
                    "reason": "",
                    "Revenue": "revenue_value",
                    "Dividend Yield": "dividend_yield_value",
                    "5-Years Growth": growth_value,
                    ... all financial metrics  given as input all original including risk metrics
                }
            ]
        }""",
        agent=financial_screening_agent
    )
except Exception as e:
    print(f"Task initialization error: {e}")
    screen_companies_task = None

# ============================================================================
# CREWAI CREW
# ============================================================================

try:
    screening_crew = Crew(
        agents=[financial_screening_agent],
        tasks=[screen_companies_task],
        process=Process.sequential,
        verbose=True,
        cache=False,
    )
except Exception as e:
    print(f"Crew initialization error: {e}")
    screening_crew = None


# ============================================================================
# RIGID JSON PARSING - HANDLES BACKTICKS & MARKDOWN
# ============================================================================
def extract_and_parse_json(result_text: str) -> dict:
    """
    RIGID JSON PARSING - Handles all formats including backticks
    """
    print(f"\n RIGID JSON PARSING STARTED")
    print(f"Result length: {len(result_text)} chars\n")

    # Strategy 1: Remove markdown backticks first
    print("Strategy 1: Removing markdown backticks...")
    cleaned_text = result_text.strip()

    # Remove ``` json ... ``` wrappers
    if cleaned_text.startswith('```'):
        cleaned_text = re.sub(r'^```(?:json)?\s*', '', cleaned_text)
        cleaned_text = re.sub(r'```\s*$', '', cleaned_text)
        cleaned_text = cleaned_text.strip()
        print("✓ Removed markdown backticks")

    # Strategy 2: Direct JSON parse
    print("Strategy 2: Direct JSON parse...")
    try:
        raw_parsed = json.loads(cleaned_text)
        if "company_details" in raw_parsed and isinstance(raw_parsed.get("company_details"), list):
            print(f"SUCCESS: Direct JSON parse - {len(raw_parsed['company_details'])} companies\n")
            return raw_parsed
    except json.JSONDecodeError as e:
        print(f"Direct JSON failed: {e}\n")

    # Strategy 3: Extract JSON between braces
    print("Strategy 3: Extracting JSON between braces...")
    start = cleaned_text.find('{')
    end = cleaned_text.rfind('}') + 1

    if start != -1 and end > start:
        json_str = cleaned_text[start:end]
        try:
            raw_parsed = json.loads(json_str)
            if "company_details" in raw_parsed and isinstance(raw_parsed.get("company_details"), list):
                print(f"SUCCESS: Brace extraction - {len(raw_parsed['company_details'])} companies\n")
                return raw_parsed
        except json.JSONDecodeError as e:
            print(f"Brace extraction failed: {e}\n")

    # Strategy 4: Look for JSON array
    print("Strategy 4: Extracting JSON array...")
    json_array_match = re.search(r'\[\s*\{.*?\}\s*\]', cleaned_text, re.DOTALL)
    if json_array_match:
        try:
            json_str = json_array_match.group(0)
            companies_array = json.loads(json_str)
            if isinstance(companies_array, list) and len(companies_array) > 0:
                print(f"SUCCESS: Array extraction - {len(companies_array)} companies\n")
                return {"company_details": companies_array}
        except json.JSONDecodeError as e:
            print(f"Array extraction failed: {e}\n")

    # Strategy 5: Remove common problematic characters
    print("Strategy 5: Cleaning problematic characters...")
    cleaned_text = cleaned_text.replace('\n', ' ').replace('\\', '')

    start = cleaned_text.find('{')
    end = cleaned_text.rfind('}') + 1

    if start != -1 and end > start:
        json_str = cleaned_text[start:end]
        try:
            raw_parsed = json.loads(json_str)
            if "company_details" in raw_parsed:
                print(f"SUCCESS: Cleaned extraction - {len(raw_parsed['company_details'])} companies\n")
                return raw_parsed
        except json.JSONDecodeError as e:
            print(f"Cleaned extraction failed: {e}\n")

    print("All parsing strategies failed\n")
    return {"company_details": []}


# ============================================================================
# ADD THIS NEW FUNCTION - EXTRACT TOKEN USAGE
# ============================================================================

import re


def extract_token_usage_dict(usage_metrics) -> dict:
    """Clean & compact token usage extractor."""
    try:
        if not usage_metrics:
            return {}

        keys = [
            "total_tokens",
            "prompt_tokens",
            "cached_prompt_tokens",
            "completion_tokens",
            "successful_requests"
        ]

        #  Object with attributes
        if hasattr(usage_metrics, "__dict__"):
            return {k: getattr(usage_metrics, k, 0) for k in keys}

        #  Dict input
        if isinstance(usage_metrics, dict):
            return {k: usage_metrics.get(k, 0) for k in keys}

        #  Fallback → parse string using regex
        usage_str = str(usage_metrics)
        return {
            key: int(m.group(1)) if (m := re.search(fr"{key}=(\d+)", usage_str)) else 0
            for key in keys
        }

    except Exception as e:
        print(f"Error extracting token usage: {e}")
        return {}


# ============================================================================
# UPDATED WEBSOCKET SCREENING FUNCTION
# ============================================================================

async def run_screening_with_websocket(
        websocket: WebSocket,
        mandate_id: int,
        mandate_parameters: dict,
        company_id_list: List[int] = None
) -> dict:
    """Run screening with REAL-TIME streaming. Companies are fetched from database by tools."""

    try:
        callback = WebSocketStreamingCallback(websocket)
        await callback.on_agent_initialized()
        await asyncio.sleep(0.1)

        if not screening_crew:
            await callback.on_error("Screening crew not initialized")
            return {"company_details": []}

        current_loop = asyncio.get_event_loop()
        original_stdout = sys.stdout
        event_capture = RealtimeEventCapture(original_stdout, callback, current_loop)
        sys.stdout = event_capture

        try:
            print("Executing crew with real-time event streaming...\n")

            mandate_id_int = int(mandate_id)

            print(f" Mandate ID being passed: {mandate_id_int}")
            print(f" Mandate Parameters: {mandate_parameters}")
            if company_id_list:
                print(f" Company IDs to screen: {company_id_list}\n")
            else:
                print(f" Company IDs: All companies for mandate will be screened\n")

            result = await asyncio.to_thread(
                screening_crew.kickoff,
                inputs={
                    "mandate_id": mandate_id_int,  # ← Pass as int
                    "mandate_parameters": mandate_parameters,
                    "company_id_list": company_id_list  # ← PASS company_id_list to agent
                }
            )

            print("\nCrew execution complete!")
            #  EXTRACT TOKEN USAGE
            token_usage_metrics = screening_crew.usage_metrics
            print(f"\nToken Usage: {token_usage_metrics}")

            token_usage_dict = extract_token_usage_dict(token_usage_metrics)
            print(f"Extracted Token Usage: {token_usage_dict}")
            # print("\nToken Usage: " + str(screening_crew.usage_metrics))
            # print("\nToken Usage from the method: " + aggregate_token_usage(result.get("messages", [])))

            try:
                # CrewOutput has attributes, not dict methods
                if hasattr(result, 'messages') and result.messages:
                    token_info = aggregate_token_usage(result.messages)
                    print(f"\nAggregated Token Usage: {json.dumps(token_info, indent=2)}")
                else:
                    print("\nNo messages attribute in CrewOutput")
            except Exception as e:
                print(f"Error aggregating token usage: {e}")

        finally:
            sys.stdout = original_stdout

        # await asyncio.sleep(1.5)

        await callback.on_screening_progress(
            f"Screening criteria applied: {len(mandate_parameters)}\n"
            f"Companies fetched and evaluated from database"
        )

        raw_text = result.raw.strip() if hasattr(result, "raw") else str(result).strip()
        parsed_result = extract_and_parse_json(raw_text)

        mandate_id_str = str(mandate_id)
        original_mandate_id = parsed_result.get("mandate_id")
        parsed_result["mandate_id"] = mandate_id_str

        if original_mandate_id != mandate_id_str:
            print(f" MANDATE ID CORRECTED:")
            print(f"   Expected: {mandate_id_str}")
            print(f"   Got from agent: {original_mandate_id}")
            print(f"    Forced to: {mandate_id_str}")

        #  ADD TOKEN USAGE TO RESULT
        parsed_result["token_usage"] = token_usage_dict
        num_qualified = len(parsed_result.get("company_details", []))

        # await asyncio.sleep(0.5)
        await callback.on_agent_finish(
            f"Screening analysis complete.\nCompanies qualified: {num_qualified}"
        )

        # await asyncio.sleep(0.5)

        #  SEND FINAL RESULT WITH TOKEN USAGE AND BOTH PASSED/CONDITIONAL COMPANIES
        final_result = {
            "mandate_id": mandate_id_str,
            "token_usage": token_usage_dict,
            "company_details": parsed_result.get("company_details", [])
        }

        final_json = json.dumps(final_result, indent=2, default=str)
        await callback.on_final_output((final_json or "")[:1000])

        return final_result


    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

        await callback.on_error(str(e))
        return {"company_details": [], "token_usage": {}}