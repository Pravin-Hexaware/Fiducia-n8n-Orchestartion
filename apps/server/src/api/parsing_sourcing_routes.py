from fastapi import APIRouter, UploadFile, File, WebSocket, WebSocketDisconnect, HTTPException, Form
import json
import queue
import asyncio
from pathlib import Path
from datetime import datetime
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import HumanMessage, ToolMessage

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pathlib import Path

import shutil
import traceback
from collections import defaultdict
from typing import Any, Dict, Iterable

from fastapi import APIRouter, HTTPException
from pathlib import Path


from agents.agent1_parse_mandate import graph as parse_agent_graph
from agents.agent2_filter_companies import create_sector_and_industry_research_agent
from database.repositories.fundRepository import FundMandateRepository
from database.repositories.ParametersRepository import ExtractedParametersRepository

router = APIRouter(prefix="/api", tags=["fund-sourcing"])


def aggregate_token_usage(messages: Iterable[Any]) -> Dict[str, Any]:
    """
    Inspect a sequence of message objects (AIMessage, ToolMessage, etc.)
    and return aggregated token usage:
      {
        "per_model": {
          "<model_name>": {"input_tokens": X, "output_tokens": Y, "total_tokens": Z},
          ...
        },
        "totals": {"input_tokens": X, "output_tokens": Y, "total_tokens": Z}
      }
    The function is defensive: many providers use different attribute names, so we
    check common fields: `usage_metadata`, `usage`, `metadata`, `extra`.
    """
    per_model = defaultdict(lambda: {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0})
    totals = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}

    def read_usage_dict(d: Dict[str, Any]) -> Dict[str, int]:
        return {
            "input_tokens": int(d.get("input_tokens", d.get("inputTokens", 0) or 0)),
            "output_tokens": int(d.get("output_tokens", d.get("outputTokens", 0) or 0)),
            "total_tokens": int(d.get("total_tokens", d.get("totalTokens", d.get("total_tokens", 0)) or 0)),
        }

    for m in messages:
        # 1) If message has usage_metadata (LangChain AIMessage uses this name)
        usage_source = None
        model_name = None

        if hasattr(m, "usage_metadata") and getattr(m, "usage_metadata"):
            usage_source = getattr(m, "usage_metadata")
            # try to detect model name if present
            model_name = getattr(m, "model", None) or usage_source.get("model") if isinstance(usage_source,
                                                                                              dict) else None

        # 2) Some providers attach .usage or .usage_data
        elif hasattr(m, "usage") and getattr(m, "usage"):
            usage_source = getattr(m, "usage")
            model_name = getattr(m, "model", None) or (
                usage_source.get("model") if isinstance(usage_source, dict) else None)

        # 3) ToolMessage or other objects may put usage in `metadata` or `extra`
        elif hasattr(m, "metadata") and isinstance(getattr(m, "metadata"), dict):
            meta = getattr(m, "metadata")
            if any(k in meta for k in ("input_tokens", "output_tokens", "total_tokens")):
                usage_source = meta
                model_name = meta.get("model") or getattr(m, "tool_name", None) or None
        elif hasattr(m, "extra") and isinstance(getattr(m, "extra"), dict):
            extra = getattr(m, "extra")
            if any(k in extra for k in ("input_tokens", "output_tokens", "total_tokens")):
                usage_source = extra
                model_name = extra.get("model") or getattr(m, "tool_name", None) or None

        # 4) If we found usage, read and accumulate
        if usage_source and isinstance(usage_source, dict):
            u = read_usage_dict(usage_source)
            key = model_name or usage_source.get("model") or "unknown"
            per_model[key]["input_tokens"] += u["input_tokens"]
            per_model[key]["output_tokens"] += u["output_tokens"]
            per_model[key]["total_tokens"] += u["total_tokens"]
            totals["input_tokens"] += u["input_tokens"]
            totals["output_tokens"] += u["output_tokens"]
            totals["total_tokens"] += u["total_tokens"]

    return {"per_model": dict(per_model), "totals": totals}


class RealtimeThinkingCallback(BaseCallbackHandler):
    """
    Captures agent thinking from the message content itself.
    Since we're using NON-STREAMING invoke, we extract thinking from the response.

    Flow:
    1. Agent outputs complete Thought/Analysis/Action text
    2. Callback extracts these from the message
    3. Emits thinking events to WebSocket
    """

    def __init__(self, event_queue: queue.Queue):
        self.event_queue = event_queue
        self.last_tool = None
        self.thought_emitted = False  # Track if we already emitted thinking for this flow
        self.llm_call_count = 0  # Count LLM calls to catch first one

    def on_llm_end(self, response, **kwargs):
        """Called after LLM finishes - extract thinking from response BEFORE tool execution."""
        self.llm_call_count += 1

        # Only emit thinking on FIRST LLM call (before any tools)
        # Skip subsequent LLM calls (after tools return)
        if self.thought_emitted:
            return

        try:
            text = None

            # Try multiple ways to extract text from different response types

            # Method 1: Direct content attribute (AIMessage, etc.)
            if hasattr(response, 'content') and isinstance(response.content, str):
                text = response.content
            # Method 2: message.content
            elif hasattr(response, 'message') and hasattr(response.message, 'content'):
                text = response.message.content
            # Method 3: generations[0][0].text (older LangChain format)
            elif hasattr(response, 'generations') and response.generations:
                gen = response.generations[0][0]
                if hasattr(gen, 'text'):
                    text = gen.text
                elif hasattr(gen, 'message') and hasattr(gen.message, 'content'):
                    text = gen.message.content

            if not text or not isinstance(text, str):
                print(f"[DEBUG] No text extracted from response type: {type(response)}")
                return

            print(f"[DEBUG] Extracted text: {text[:200]}...")
            text_lower = text.lower()

            # Extract THOUGHT
            if "thought:" in text_lower:
                thought_start = text_lower.find("thought:") + len("thought:")
                thought_end = len(text)

                for marker in ["analysis:", "action:"]:
                    idx = text_lower.find(marker, thought_start)
                    if idx != -1:
                        thought_end = min(thought_end, idx)

                thought_text = text[thought_start:thought_end].strip()
                if thought_text and len(thought_text) > 5:
                    print(f"\u2705 THOUGHT: {thought_text[:150]}")
                    self.event_queue.put({
                        "type": "agent_thinking",
                        "step": "thought",
                        "content": thought_text,
                        "timestamp": datetime.now().isoformat()
                    })

            # Extract ANALYSIS
            if "analysis:" in text_lower:
                analysis_start = text_lower.find("analysis:") + len("analysis:")
                analysis_end = len(text)

                idx = text_lower.find("action:", analysis_start)
                if idx != -1:
                    analysis_end = idx

                analysis_text = text[analysis_start:analysis_end].strip()
                if analysis_text and len(analysis_text) > 5:
                    print(f"\u2705 ANALYSIS: {analysis_text[:150]}")
                    self.event_queue.put({
                        "type": "agent_thinking",
                        "step": "analysis",
                        "content": analysis_text,
                        "timestamp": datetime.now().isoformat()
                    })

            # Extract ACTION
            if "action:" in text_lower:
                action_start = text_lower.find("action:") + len("action:")
                action_end = len(text)

                for marker in ["tool call:", "observation:", "\n\n"]:
                    idx = text_lower.find(marker, action_start)
                    if idx != -1:
                        action_end = min(action_end, idx)

                action_text = text[action_start:action_end].strip()
                if action_text and len(action_text) > 2 and action_text.lower() != "none":
                    print(f"\u2705 ACTION: {action_text[:150]}")
                    self.event_queue.put({
                        "type": "agent_thinking",
                        "step": "action",
                        "content": action_text,
                        "timestamp": datetime.now().isoformat()
                    })

            # Mark that we've emitted thinking to skip subsequent LLM calls
            self.thought_emitted = True
        except Exception as e:
            print(f"Error in on_llm_end: {e}")
            import traceback
            traceback.print_exc()

    def on_tool_start(self, serialized: dict, input_str: str, **kwargs):
        """Tool execution start."""
        tool_name = serialized.get("name", "unknown")
        self.last_tool = tool_name
        print(f"\u2705 TOOL START: {tool_name}")

        self.event_queue.put({
            "type": "tool_start",
            "tool": tool_name,
            "message": f"Tool start: Starting {tool_name}...",
            "timestamp": datetime.now().isoformat()
        })

    def on_tool_end(self, output: str, **kwargs):
        """Tool execution end."""
        tool_name = self.last_tool or "tool"
        print(f"\u2705 TOOL END: {tool_name}")

        self.event_queue.put({
            "type": "tool_end",
            "tool": tool_name,
            "message": f"Tool Execution : {tool_name} Processing...",
            "timestamp": datetime.now().isoformat()
        })

    def on_tool_error(self, error, **kwargs):
        """Tool error."""
        tool_name = self.last_tool or "tool"
        self.event_queue.put({
            "type": "tool_error",
            "tool": tool_name,
            "error": str(error),
            "timestamp": datetime.now().isoformat()
        })


@router.post("/parse-mandate-upload")
async def parse_mandate_upload(
        file: UploadFile = File(...),
        query: str = Form("Generate mandate criteria"),
        legal_name: str = Form(...),
        strategy_type: str = Form(...),
        vintage_year: str = Form(...),
        primary_analyst: str = Form(...),
        processing_date: str = Form(None),
        target_count: str = Form(None),
        description: str = Form(...)
):
    """
    Upload PDF file + fund details via REST
    Creates database entry for FundMandate and saves file

    Returns: {"status": "success", "mandate_id": 1, "filename": "...", "legal_name": "...", "file_path": "...", "message": "..."}
    """
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files allowed")

    try:
        folder = Path(__file__).parent.parent / "input_fund_mandate"
        folder.mkdir(parents=True, exist_ok=True)

        file_path = folder / file.filename

        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        # Convert vintage_year to int
        try:
            vintage_year_int = int(vintage_year) if vintage_year else None
        except ValueError:
            vintage_year_int = None

        # Create database entry for FundMandate
        mandate = await FundMandateRepository.create_mandate(
            legal_name=legal_name,
            strategy_type=strategy_type,
            vintage_year=vintage_year_int,
            primary_analyst=primary_analyst,
            processing_date=processing_date,
            target_count=target_count,
            description=description
        )

        return {
            "status": "success",
            "mandate_id": mandate.id,
            "filename": file.filename,
            "legal_name": mandate.legal_name,
            "strategy_type": mandate.strategy_type,
            "vintage_year": mandate.vintage_year,
            "primary_analyst": mandate.primary_analyst,
            "target_count": mandate.target_count,
            "file_path": str(file_path),
            "query": query,
            "message": f"Fund mandate created and file saved: {file.filename}"
        }

    except Exception as e:
        import traceback
        print(f"Error in parse_mandate_upload: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# new 2 routes...without thinking

# ==================== HTTP: PARSE MANDATE (NO STREAM / NO THINKING) ====================
@router.post("/parse-mandate/no-stream") #capabilty route
async def parse_mandate_no_stream(payload: dict):
    """HTTP endpoint that runs the parse mandate agent synchronously and returns results.

    Expects JSON body with keys: `pdf_name`, `query` (optional), `capability_params` (optional), `mandate_id` (optional)
    This endpoint does NOT stream thinking events or use callbacks; it simply invokes the agent and returns the final result.
    """
    try:
        pdf_name = payload.get("pdf_name")
        query = payload.get("query", "Scan input_fund_mandate and extract criteria")
        capability_params = payload.get("capability_params", {})
        mandate_id = payload.get("mandate_id")

        if not pdf_name:
            raise HTTPException(status_code=400, detail="Missing 'pdf_name'")

        folder = Path(__file__).parent.parent / "input_fund_mandate"
        pdf_path = folder / pdf_name
        if not pdf_path.exists():
            raise HTTPException(status_code=404, detail=f"File not found: {pdf_name}")

        agent = parse_agent_graph
        input_data = {
            "messages": [HumanMessage(content=query)],
            "pdf_name": pdf_name,
            "query": query,
            "capability_params": capability_params,
        }

        config = {
            "configurable": {
                "recursion_limit": 50,
                "thread_id": f"parse-no-stream-{int(datetime.now().timestamp() * 1000)}",
            }
        }

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: agent.invoke(input_data, config))

        tokens_info = aggregate_token_usage(result.get("messages", []))

        criteria = {}
        if result.get("messages"):
            tool_messages = [m for m in result["messages"] if isinstance(m, ToolMessage)]
            if tool_messages:
                last_tool_msg = tool_messages[-1]
                try:
                    content = last_tool_msg.content
                    if "```" in content:
                        start = content.find("```")
                        end = content.rfind("```")
                        if start != -1 and end != -1 and start != end:
                            content = content[start + 3:end].strip()
                            if content.startswith("json"):
                                content = content[4:].strip()
                    criteria = json.loads(content)
                except Exception:
                    criteria = {"raw_output": last_tool_msg.content}
            else:
                final_msg = result["messages"][-1]
                if hasattr(final_msg, 'content') and isinstance(final_msg.content, str):
                    try:
                        if "{" in final_msg.content and "}" in final_msg.content:
                            json_start = final_msg.content.find("{")
                            json_end = final_msg.content.rfind("}") + 1
                            json_str = final_msg.content[json_start:json_end]
                            criteria = json.loads(json_str)
                    except Exception:
                        criteria = {"raw_output": final_msg.content[:200]}

        # Persist extracted parameters to database if analysis was successful and mandate_id provided
        persisted_id = None
        if criteria and mandate_id:
            try:
                extracted_params = await ExtractedParametersRepository.create_extracted_parameters(
                    criteria=criteria,
                    fund_mandate_id=mandate_id
                )
                if extracted_params:
                    persisted_id = extracted_params.id
                    await FundMandateRepository.update_extracted_parameters(
                        mandate_id=mandate_id,
                        extracted_parameters_id=extracted_params.id
                    )
            except Exception as e:
                print(f"Error persisting extracted parameters (no-stream): {e}")

        return {
            "status": "success",
            "criteria": criteria,
            "tokens_used": tokens_info,
            "persisted_extracted_parameters_id": persisted_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in parse_mandate_no_stream: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== HTTP: FILTER COMPANIES (NO STREAM / NO THINKING) ====================


@router.post("/filter-companies/no-stream")
async def filter_companies_no_stream(user_filters: dict):
    """HTTP endpoint that runs the sector & industry research agent and returns results.

    Expects JSON body matching the websocket: {"mandate_id": <int>, "additionalProp1": {...}}
    This endpoint does NOT stream thinking events or use callbacks; it simply invokes the agent and returns the final result.
    """
    try:
        if not user_filters:
            raise HTTPException(status_code=400, detail="Filter data is required")

        mandate_id = user_filters.get("mandate_id")
        if not mandate_id:
            raise HTTPException(status_code=400,
                                detail='mandate_id is required. Format: {"mandate_id": 13, "additionalProp1": {...}}')

        agent = create_sector_and_industry_research_agent()
        input_data = {"messages": [HumanMessage(content=json.dumps(user_filters))]}

        config = {
            "configurable": {
                "recursion_limit": 50,
                "thread_id": f"filter-no-stream-{int(datetime.now().timestamp() * 1000)}",
            }
        }

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: agent.invoke(input_data, config))

        tokens_info = aggregate_token_usage(result.get("messages", []))

        companies = {}
        if result.get("messages"):
            tool_messages = [m for m in result["messages"] if isinstance(m, ToolMessage)]
            if tool_messages:
                try:
                    content = tool_messages[0].content
                    if "```" in content:
                        start = content.find("```")
                        end = content.rfind("```")
                        if start != -1 and end != -1 and start != end:
                            content = content[start + 3:end].strip()
                            if content.startswith("json"):
                                content = content[4:].strip()
                    companies = json.loads(content)
                except Exception:
                    companies = {"raw_output": tool_messages[0].content}

        qualified = []
        if isinstance(companies, dict):
            qualified = companies.get("qualified", []) or []
        elif isinstance(companies, list):
            qualified = companies

        company_names = []
        Sourced_ids=[]
        for c in qualified:
            if isinstance(c, dict):
                c_id = (c.get("id") or c.get("company_id"))
                name = (c.get("name") or c.get("company") or c.get("Company") or
                        c.get("Company ") or c.get("company_name") or c.get("Name"))
                company_names.append(name if name else str(c))
                Sourced_ids.append(c_id)
            else:
                company_names.append(str(c))

        return {
            "status": "success",
            "result": Sourced_ids,
            #"sourced_ids": Sourced_ids,
            "filtered_company_count": len(qualified),
            #"companies": company_names,
            #"tokens_used": tokens_info,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in filter_companies_no_stream: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# ENDPOINT 1: PARSE MANDATE + UPLOAD (New - Integrated)
#======================================================

class MandateUpload(BaseModel):
    filename: str
    query: str
    legal_name: str
    strategy_type: str
    vintage_year: str | None = None
    primary_analyst: str | None = None
    processing_date: str | None = None
    target_count: str | None = None
    description: str | None = None

@router.post("/mandate-upload")
async def mandate_upload(payload: MandateUpload):
    try:
        if not payload.filename.endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Only PDF files allowed")

        folder = Path(__file__).parent.parent / "input_fund_mandate"
        file_path = folder / payload.filename

        # Convert vintage_year to int
        try:
            vintage_year_int = int(payload.vintage_year) if payload.vintage_year else None
        except ValueError:
            vintage_year_int = None

        mandate = await FundMandateRepository.create_mandate(
            legal_name=payload.legal_name,
            strategy_type=payload.strategy_type,
            vintage_year=vintage_year_int,
            primary_analyst=payload.primary_analyst,
            processing_date=payload.processing_date,
            target_count=payload.target_count,
            description=payload.description,
        )

        return {
            "status": "success",
            "mandate_id": mandate.id,
            "filename": payload.filename,
            "legal_name": mandate.legal_name,
            "strategy_type": mandate.strategy_type,
            "vintage_year": mandate.vintage_year,
            "primary_analyst": mandate.primary_analyst,
            "target_count": mandate.target_count,
            "file_path": str(file_path),
            "query": payload.query,
            "message": f"Fund mandate created and file saved: {payload.filename}",
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"Error in parse_mandate_upload: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")



# ==================================================
# ENDPOINT 2: REST FILE UPLOAD (Legacy)
# ==================================================

@router.post("/upload-mandate")
async def upload_mandate(file: UploadFile = File(...)):
    """
    Upload PDF file via REST

    curl -X POST http://localhost:8000/api/upload-mandate -F "file=@path/to/file.pdf"

    Returns: {"status": "success", "filename": "...", "path": "..."}
    """
    if not file.filename.endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files allowed")

    try:
        folder = Path(__file__).parent.parent / "input_fund_mandate"
        folder.mkdir(parents=True, exist_ok=True)

        file_path = folder / file.filename

        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        return {
            "status": "success",
            "filename": file.filename,
            "path": str(file_path),
            "message": f"File saved: {file.filename}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@router.get("/health/option2")
async def health_option2():
    return {"status": "healthy", "option": "2 - REST Upload + WebSocket"}


# ==================== WEBSOCKET 1: PARSE MANDATE ====================

@router.websocket("/ws/parse-mandate/option2/{session_id}")
async def ws_parse_mandate_realtime(websocket: WebSocket, session_id: str):
    """Parse mandate PDF with real-time thinking events."""
    await websocket.accept()
    event_queue = queue.Queue()

    try:
        event_queue.put({
            "type": "session_start",
            "message": "Mandate Parsing Agent initialized",
            "session_id": session_id,
            "timestamp": datetime.now().isoformat()
        })

        async def stream_events():
            while True:
                try:
                    event = event_queue.get_nowait()
                    if event is None:
                        break
                    await websocket.send_json(event)
                except queue.Empty:
                    await asyncio.sleep(0.01)

        streaming_task = asyncio.create_task(stream_events())

        msg = await websocket.receive_json()
        pdf_name = msg.get("pdf_name")
        query = msg.get("query", "Scan input_fund_mandate and extract criteria")
        capability_params = msg.get("capability_params", {})
        mandate_id = msg.get("mandate_id")  # Optional: for linking extracted parameters

        if not pdf_name:
            event_queue.put({
                "type": "error",
                "message": "Missing 'pdf_name'",
                "timestamp": datetime.now().isoformat()
            })
            event_queue.put(None)
            await streaming_task
            return

        folder = Path(__file__).parent.parent / "input_fund_mandate"
        pdf_path = folder / pdf_name

        if not pdf_path.exists():
            event_queue.put({
                "type": "error",
                "message": f"File not found: {pdf_name}",
                "timestamp": datetime.now().isoformat()
            })
            event_queue.put(None)
            await streaming_task
            return

        event_queue.put({
            "type": "analysis_start",
            "message": f"File loaded: {pdf_name}",
            "pdf_path": str(pdf_path),
            "timestamp": datetime.now().isoformat()
        })

        try:
            agent = parse_agent_graph
            input_data = {
                "messages": [HumanMessage(content=query)],
                "pdf_name": pdf_name,
                "query": query,
                "capability_params": capability_params
            }

            config = {
                "callbacks": [RealtimeThinkingCallback(event_queue)],
                "configurable": {
                    "recursion_limit": 50,
                    "thread_id": f"parse-{session_id}-{int(datetime.now().timestamp() * 1000)}"  # Unique ID - NO CACHE
                }
            }

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: agent.invoke(input_data, config)
            )

            # Aggregate tokens from run messages
            tokens_info = aggregate_token_usage(result.get("messages", []))

            criteria = {}
            if result.get("messages"):
                # Extract TOOL OUTPUT from LAST ToolMessage - this ensures we get the final tool output
                tool_messages = [msg for msg in result["messages"] if isinstance(msg, ToolMessage)]

                if tool_messages:
                    # Use LAST tool message (final tool output)
                    last_tool_msg = tool_messages[-1]
                    try:
                        content = last_tool_msg.content

                        # Clean markdown code blocks if present
                        if "```" in content:
                            # Extract content between ``` markers
                            start = content.find("```")
                            end = content.rfind("```")
                            if start != -1 and end != -1 and start != end:
                                content = content[start + 3:end].strip()
                                # Remove "json" prefix if present
                                if content.startswith("json"):
                                    content = content[4:].strip()

                        # Parse JSON
                        criteria = json.loads(content)
                        print(f"\n✅ EXTRACTED TOOL OUTPUT: {json.dumps(criteria)[:200]}")
                    except (json.JSONDecodeError, ValueError) as e:
                        print(f"JSON parse error: {e}")
                        criteria = {"raw_output": last_tool_msg.content}
                else:
                    # Fallback: parse final message
                    final_msg = result["messages"][-1]
                    if hasattr(final_msg, 'content') and isinstance(final_msg.content, str):
                        try:
                            if "{" in final_msg.content and "}" in final_msg.content:
                                json_start = final_msg.content.find("{")
                                json_end = final_msg.content.rfind("}") + 1
                                json_str = final_msg.content[json_start:json_end]
                                criteria = json.loads(json_str)
                        except (json.JSONDecodeError, ValueError):
                            criteria = {"raw_output": final_msg.content[:200]}

            event_queue.put({
                "type": "analysis_complete",
                "status": "success",
                "criteria": criteria,
                "message": "Mandate Parsing Agent completed analysis!",
                "tokens_used": tokens_info,
                "timestamp": datetime.now().isoformat()
            })

            # Persist extracted parameters to database if analysis was successful
            if criteria and mandate_id:
                try:
                    extracted_params = await ExtractedParametersRepository.create_extracted_parameters(
                        criteria=criteria,
                        fund_mandate_id=mandate_id
                    )
                    if extracted_params:
                        # Link the extracted parameters to the mandate
                        await FundMandateRepository.update_extracted_parameters(
                            mandate_id=mandate_id,
                            extracted_parameters_id=extracted_params.id
                        )
                        # Update the event with the extracted parameters ID
                        event_queue.put({
                            "type": "parameters_persisted",
                            "extracted_parameters_id": extracted_params.id,
                            "message": "Extracted parameters successfully persisted and linked to mandate",
                            "timestamp": datetime.now().isoformat()
                        })
                except Exception as e:
                    print(f"Error persisting extracted parameters: {e}")
                    print(traceback.format_exc())
                    event_queue.put({
                        "type": "persistence_error",
                        "error": str(e),
                        "message": "Failed to persist extracted parameters",
                        "timestamp": datetime.now().isoformat()
                    })

        except Exception as e:
            import traceback
            event_queue.put({
                "type": "analysis_complete",
                "status": "error",
                "error": str(e),
                "message": f"Parsing failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })

        event_queue.put({
            "type": "session_complete",
            "status": "success",
            "message": "Mandate Parsing Agent session finished!",
            "timestamp": datetime.now().isoformat()
        })
        event_queue.put(None)

        await streaming_task

    except WebSocketDisconnect:
        print(f"WS disconnected: {session_id}")
    except Exception as e:
        print(f"Error in ws_parse_mandate_realtime: {e}")
        try:
            event_queue.put({
                "type": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            })
            event_queue.put(None)
        except Exception:
            pass


# ==================== WEBSOCKET 2: FILTER COMPANIES ====================

@router.websocket("/ws/filter-companies/option2/{session_id}")
async def ws_filter_companies_realtime(websocket: WebSocket, session_id: str):
    """Filter companies with real-time thinking events."""
    await websocket.accept()
    event_queue = queue.Queue()

    try:
        event_queue.put({
            "type": "session_start",
            "message": "Sector & Industry Research Agent initialized",
            "session_id": session_id,
            "timestamp": datetime.now().isoformat()
        })

        async def stream_events():
            while True:
                try:
                    event = event_queue.get_nowait()
                    if event is None:
                        break
                    await websocket.send_json(event)
                except queue.Empty:
                    await asyncio.sleep(0.01)

        streaming_task = asyncio.create_task(stream_events())

        data = await websocket.receive_json()
        user_filters = data

        if not user_filters:
            event_queue.put({
                "type": "error",
                "message": "Filter data is required",
                "timestamp": datetime.now().isoformat()
            })
            event_queue.put(None)
            await streaming_task
            return

        event_queue.put({
            "type": "analysis_start",
            "message": "Filters received and validated",
            "filter_count": len(user_filters),
            "filters": user_filters,
            "timestamp": datetime.now().isoformat()
        })

        try:
            agent = create_sector_and_industry_research_agent()
            mandate_id = user_filters.get("mandate_id")
            if not mandate_id:
                event_queue.put({
                    "type": "error",
                    "message": "mandate_id is required. Format: {\"mandate_id\": 13, \"additionalProp1\": {...}}",
                    "timestamp": datetime.now().isoformat()
                })
                event_queue.put(None)
                await streaming_task
                return
            input_data = {
                "messages": [HumanMessage(content=json.dumps(user_filters))]
            }

            config = {
                "callbacks": [RealtimeThinkingCallback(event_queue)],
                "configurable": {
                    "recursion_limit": 50,
                    "thread_id": f"filter-{session_id}-{int(datetime.now().timestamp() * 1000)}"  # Unique ID - NO CACHE
                }
            }

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: agent.invoke(input_data, config)
            )

            # Aggregate tokens from run messages
            tokens_info = aggregate_token_usage(result.get("messages", []))

            companies = {}
            if result.get("messages"):
                tool_messages = [m for m in result["messages"] if isinstance(m, ToolMessage)]
                if tool_messages:
                    try:
                        # Get FIRST tool message (the actual tool output)
                        content = tool_messages[0].content

                        # Clean markdown code blocks if present
                        if "```" in content:
                            start = content.find("```")
                            end = content.rfind("```")
                            if start != -1 and end != -1 and start != end:
                                content = content[start + 3:end].strip()
                                if content.startswith("json"):
                                    content = content[4:].strip()

                        # Parse JSON
                        companies = json.loads(content)
                        print(f"\n✅ EXTRACTED TOOL OUTPUT (Route 2): {json.dumps(companies)[:200]}")
                    except (json.JSONDecodeError, ValueError) as e:
                        print(f"JSON parse error: {e}")
                        companies = {"raw_output": tool_messages[0].content}

            qualified = []
            if isinstance(companies, dict):
                qualified = companies.get("qualified", []) or []
            elif isinstance(companies, list):
                qualified = companies

            company_names = []
            for c in qualified:
                if isinstance(c, dict):
                    name = (c.get("name") or c.get("company") or c.get("Company") or
                            c.get("Company ") or c.get("company_name") or c.get("Name"))
                    company_names.append(name if name else str(c))
                else:
                    company_names.append(str(c))

            event_queue.put({
                "type": "analysis_complete",
                "status": "success",
                "result": companies,
                "filtered_company_count": len(qualified),
                "companies": company_names,
                "message": f"Sector & Industry Research Agent found {len(qualified)} matches!",
                "tokens_used": tokens_info,
                "timestamp": datetime.now().isoformat()
            })

        except Exception as e:
            import traceback
            event_queue.put({
                "type": "analysis_complete",
                "status": "error",
                "error": str(e),
                "message": f"Filtering failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })

        event_queue.put({
            "type": "session_complete",
            "status": "success",
            "message": "Sector & Industry Research Agent session finished!",
            "timestamp": datetime.now().isoformat()
        })
        event_queue.put(None)

        await streaming_task

    except WebSocketDisconnect:
        print(f"WS disconnected: {session_id}")
    except Exception as e:
        print(f"Error in ws_filter_companies_realtime: {e}")
        try:
            event_queue.put({
                "type": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            })
            event_queue.put(None)
        except Exception:
            pass