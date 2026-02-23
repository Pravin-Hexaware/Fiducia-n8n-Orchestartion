# import json
# import traceback
# from typing import List, Dict, Any
# from azure.ai.agents.models import ListSortOrder
# from datetime import datetime
# from azure.ai.projects import AIProjectClient
# from azure.identity import DefaultAzureCredential
# from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
# from pydantic import BaseModel
#
# # Import CrewAI components from mandate_screening
# try:
#     from agents.mandate_screening import screening_crew, run_screening_with_websocket
# except Exception as e:
#     print(f"Error importing mandate_screening: {e}")
#     screening_crew = None
#
# # Import screening repository
# from database.repositories.screeningRepository import ScreeningRepository
#
# PROJECT_ENDPOINT = "https://fstoaihub1292141971.services.ai.azure.com/api/projects/fstoaihub1292141971-AgentsSample"
# AGENT_ID = "asst_Nm4bdHLpmI2W2VdwT2lF1Jr8"
#
#
# class QueryRequest(BaseModel):
#     content: str
#
#
# class QueryResponse(BaseModel):
#     response: str
#     status: str
#
#
# class ScreeningRequest(BaseModel):
#     """Financial Screening Request Model"""
#     mandate_parameters: dict
#     companies: List[dict]
#
#     class Config:
#         json_schema_extra = {
#             "example": {
#                 "mandate_parameters": {
#                     "revenue": "> 40000000",
#                     "debt_to_equity": "< 0.5",
#                     "pe_ratio": "< 40"
#                 },
#                 "companies": [
#                     {
#                         "Company": "Microsoft",
#                         "Sector": "Technology",
#                         "Revenue": 281724.0,
#                         "Debt / Equity": 0.3315,
#                         "P/E Ratio": 34.47
#                     }
#                 ]
#             }
#         }
#
#
# class ScreeningResponse(BaseModel):
#     """API Response Model - Wrapped Format"""
#     company_details: List[Dict[str, Any]]
#
#
# router = APIRouter()
#
#
# def get_project_client():
#     try:
#         client = AIProjectClient(
#             credential=DefaultAzureCredential(),
#             endpoint=PROJECT_ENDPOINT
#         )
#         return client
#     except Exception as e:
#         print(f" Azure client not available: {e}")
#         return None
#
#
# def query_agent(user_content: str) -> dict:
#     project = get_project_client()
#
#     if not project:
#         return {
#             "response": "Azure AI Project is not initialized",
#             "status": "error"
#         }
#
#     try:
#         thread = project.agents.threads.create()
#
#         project.agents.messages.create(
#             thread_id=thread.id,
#             role="user",
#             content=user_content
#         )
#
#         run = project.agents.runs.create_and_process(
#             thread_id=thread.id,
#             agent_id=AGENT_ID
#         )
#
#         if run.status == "failed":
#             return {
#                 "response": f"Agent run failed: {run.last_error}",
#                 "status": "error"
#             }
#
#         messages = project.agents.messages.list(
#             thread_id=thread.id,
#             order=ListSortOrder.ASCENDING
#         )
#
#         agent_response = None
#         for message in messages:
#             if message.role == "assistant" and message.text_messages:
#                 agent_response = message.text_messages[-1].text.value
#
#         if not agent_response:
#             return {
#                 "response": "No response from agent",
#                 "status": "error"
#             }
#
#         return {
#             "response": agent_response,
#             "status": "success"
#         }
#
#     except Exception as e:
#         return {
#             "response": f"Error processing query: {str(e)}",
#             "status": "error"
#         }
#
#
# @router.post("/chat", response_model=QueryResponse)
# async def chat(request: QueryRequest) -> QueryResponse:
#     """
#     Send a query to the Azure agent and get a response
#     """
#     result = query_agent(request.content)
#     return QueryResponse(
#         response=result["response"],
#         status=result["status"]
#     )
#
#
# @router.websocket("/api/ws/screen")
# async def websocket_screen_companies(websocket: WebSocket):
#     """WebSocket endpoint for real-time company screening with streaming.
#
#     ✅ FIXED: Properly handles mandate_id from client request
#     """
#     await websocket.accept()
#
#     try:
#         print("\n" + "=" * 80)
#         print("🔴 WEBSOCKET CONNECTION ESTABLISHED")
#         print("=" * 80)
#
#         print("⏳ Waiting for client request...")
#         data = await websocket.receive_json()
#
#         # ✅ EXTRACT mandate_id FROM CLIENT REQUEST
#         mandate_id = data.get("mandate_id")  # ← GET FROM CLIENT
#         mandate_parameters = data.get("mandate_parameters", {})
#         companies = data.get("companies", [])
#
#         print(f"\n✅ RECEIVED REQUEST FROM CLIENT:")
#         print(f"   - Mandate ID: {mandate_id}")
#         print(f"   - Mandate Parameters: {len(mandate_parameters)} criteria")
#         print(f"   - Companies: {len(companies)} companies\n")
#
#         # Validate input
#         if not mandate_id or not mandate_parameters or not companies:
#             await websocket.send_json({
#                 "type": "error",
#                 "content": "Invalid request: mandate_id, mandate_parameters, and companies are required"
#             })
#             await websocket.close(code=1008)
#             return
#
#         # ✅ Convert mandate_id to string for consistency
#         mandate_id_str = str(mandate_id).strip()
#         print(f"🔒 STORED mandate_id: {mandate_id_str} (type: {type(mandate_id).__name__})")
#
#         # Notify client
#         await websocket.send_json({
#             "type": "info",
#             "content": f"Screening started for mandate ID: {mandate_id_str}"
#         })
#
#         # ✅ RUN SCREENING WITH CORRECT mandate_id
#         result = await run_screening_with_websocket(
#             websocket=websocket,
#             mandate_id=mandate_id_str,  # ← USE CLIENT's mandate_id
#             mandate_parameters=mandate_parameters,
#             companies=companies
#         )
#
#         # ✅ GUARANTEE mandate_id IN RESULT
#         result["mandate_id"] = mandate_id_str
#         print(f"✅ Result mandate_id guaranteed: {result.get('mandate_id')}")
#
#         # Extract results
#         company_details = result.get("company_details", [])
#
#         # STEP 3: Save company screening results
#         if company_details:
#             screenings = await ScreeningRepository.process_agent_output(
#                 fund_mandate_id=mandate_id_str,
#                 selected_parameters=mandate_parameters,
#                 company_details=company_details,
#                 raw_agent_output=json.dumps(result)
#             )
#
#             print(f" Created {len(screenings)} screening records")
#             print(f"   Mandate ID: {mandate_id_str}")
#             print(f"   Companies screened: {len(company_details)}")
#
#             await websocket.send_json({
#                 "type": "info",
#                 "content": f" Stored {len(screenings)} screening results"
#             })
#         else:
#             print("️ No company details in agent output")
#
#         # STEP 4: Send final result back to client
#         await websocket.send_json({
#             "type": "final_result",
#             "content": result
#         })
#
#         print("WEBSOCKET SESSION COMPLETED SUCCESSFULLY\n")
#
#     except WebSocketDisconnect:
#         print("WebSocket client disconnected")
#
#     except Exception as e:
#         print(f" WebSocket error: {str(e)}")
#         import traceback
#         traceback.print_exc()
#
#         try:
#             await websocket.send_json({
#                 "type": "error",
#                 "content": f"Server error: {str(e)}"
#             })
#         except:
#             pass
#
#         finally:
#             try:
#                 await websocket.close(code=1011)
#             except:
#                 pass

import json
import traceback
import asyncio
from typing import List, Dict, Any
from azure.ai.agents.models import ListSortOrder
from datetime import datetime
from agents.mandate_screening import run_screening_with_websocket
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

# Import CrewAI components from mandate_screening
try:
    from agents.mandate_screening import (
        screening_crew,
        run_screening_with_websocket,
        extract_token_usage_dict,
        extract_and_parse_json
    )
except Exception as e:
    print(f"Error importing mandate_screening: {e}")
    screening_crew = None

# Import screening repository
from database.repositories.screeningRepository import ScreeningRepository

PROJECT_ENDPOINT = "https://fstoaihub1292141971.services.ai.azure.com/api/projects/fstoaihub1292141971-AgentsSample"
AGENT_ID = "asst_Nm4bdHLpmI2W2VdwT2lF1Jr8"


class QueryRequest(BaseModel):
    content: str


class QueryResponse(BaseModel):
    response: str
    status: str


class ScreeningRequest(BaseModel):
    """Financial Screening Request Model - Database-Driven with Company IDs"""
    mandate_id: int
    mandate_parameters: dict
    company_id: List[int] = None  # Optional: specific companies to screen

    class Config:
        json_schema_extra = {
            "example": {
                "mandate_id": 1,
                "mandate_parameters": {
                    "revenue": "> $40M USD",
                    "debt_to_equity": "< 0.5",
                    "pe_ratio": "< 40"
                },
                "company_id": [1, 2, 3, 5]
            }
        }


class ScreeningResponse(BaseModel):
    """API Response Model - Wrapped Format"""
    company_details: List[Dict[str, Any]]


router = APIRouter()


def get_project_client():
    try:
        client = AIProjectClient(
            credential=DefaultAzureCredential(),
            endpoint=PROJECT_ENDPOINT
        )
        return client
    except Exception as e:
        print(f"Azure client not available: {e}")
        return None


def query_agent(user_content: str) -> dict:
    project = get_project_client()

    if not project:
        return {
            "response": "Azure AI Project is not initialized",
            "status": "error"
        }

    try:
        thread = project.agents.threads.create()

        project.agents.messages.create(
            thread_id=thread.id,
            role="user",
            content=user_content
        )

        run = project.agents.runs.create_and_process(
            thread_id=thread.id,
            agent_id=AGENT_ID
        )

        if run.status == "failed":
            return {
                "response": f"Agent run failed: {run.last_error}",
                "status": "error"
            }

        messages = project.agents.messages.list(
            thread_id=thread.id,
            order=ListSortOrder.ASCENDING
        )

        agent_response = None
        for message in messages:
            if message.role == "assistant" and message.text_messages:
                agent_response = message.text_messages[-1].text.value

        if not agent_response:
            return {
                "response": "No response from agent",
                "status": "error"
            }

        return {
            "response": agent_response,
            "status": "success"
        }

    except Exception as e:
        return {
            "response": f"Error processing query: {str(e)}",
            "status": "error"
        }


@router.post("/chat", response_model=QueryResponse)
async def chat(request: QueryRequest) -> QueryResponse:
    """
    Send a query to the Azure agent and get a response
    """
    result = query_agent(request.content)
    return QueryResponse(
        response=result["response"],
        status=result["status"]
    )


@router.websocket("/api/ws/screen")
async def websocket_screen_companies(websocket: WebSocket):
    """WebSocket endpoint for real-time company screening with streaming."""
    await websocket.accept()

    try:
        print("[WS] Connection established")

        # Receive request from client
        data = await websocket.receive_json()

        # ✅ EXTRACT mandate_id, mandate_parameters, AND company_id
        mandate_id = data.get("mandate_id")
        mandate_parameters = data.get("mandate_parameters", {})
        company_id_list = data.get("company_id", None)

        # Validate input
        if not mandate_id or not mandate_parameters:
            await websocket.send_json({
                "type": "error",
                "content": "Invalid request: mandate_id and mandate_parameters are required"
            })
            await websocket.close(code=1008)
            return

        # ✅ Convert mandate_id to int
        mandate_id_int = int(mandate_id)

        # Notify client
        await websocket.send_json({
            "type": "info",
            "content": f"Screening started for mandate ID: {mandate_id_int}"
        })

        # ✅ RUN SCREENING
        result = await run_screening_with_websocket(
            websocket=websocket,
            mandate_id=mandate_id_int,
            mandate_parameters=mandate_parameters,
            company_id_list=company_id_list
        )

        # ✅ GUARANTEE mandate_id IN RESULT
        result["mandate_id"] = mandate_id_int

        # Extract results
        company_details = result.get("company_details", [])

        # ✅ SAVE RESULTS TO DATABASE
        if company_details:
            try:
                await ScreeningRepository.process_agent_output(
                    fund_mandate_id=mandate_id_int,
                    selected_parameters=mandate_parameters,
                    company_details=company_details,
                    raw_agent_output=json.dumps(result)
                )
            except Exception as db_error:
                print(f"[WS] Database save warning: {db_error}")

        # ✅ SEND FINAL RESULT TO CLIENT
        await websocket.send_json({
            "type": "final_result",
            "content": result
        })

        print("[WS] Session completed successfully")

    except WebSocketDisconnect:
        print("[WS] Client disconnected")

    except Exception as e:
        print(f"[WS] Error: {str(e)}")
        traceback.print_exc()

        try:
            await websocket.send_json({
                "type": "error",
                "content": f"Server error: {str(e)}"
            })
        except:
            pass

        try:
            await websocket.close(code=1011)
        except:
            pass


# ============================================================================
# HTTP POST ENDPOINT - FULL FUNCTIONALITY (MATCHING WEBSOCKET)
# ============================================================================

@router.post("/api/screen-companies", response_model=dict)
async def screen_companies_endpoint(request: ScreeningRequest):
    """
    Screen companies with FULL functionality matching WebSocket endpoint.
    Includes: token extraction, database saving, complete agent workflow.
    """
    try:
        # VALIDATE INPUT
        if not request.mandate_id:
            raise HTTPException(status_code=400, detail="mandate_id is required")

        if not request.mandate_parameters:
            raise HTTPException(status_code=400, detail="mandate_parameters cannot be empty")

        if not screening_crew:
            raise HTTPException(status_code=500, detail="CrewAI screening crew not initialized")

        # PREPARE INPUT
        mandate_id_int = int(request.mandate_id)
        company_id_list = request.company_id

        print(
            f"\n[HTTP] Starting screening - mandate_id={mandate_id_int}, companies={len(company_id_list) if company_id_list else 'all'}")

        # RUN CREW IN THREAD POOL (Non-blocking)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            screening_crew.kickoff,
            {
                "mandate_id": mandate_id_int,
                "mandate_parameters": request.mandate_parameters,
                "company_id_list": company_id_list
            }
        )

        # EXTRACT TOKEN USAGE
        token_usage_dict = {}
        if hasattr(screening_crew, 'usage_metrics') and screening_crew.usage_metrics:
            token_usage_metrics = screening_crew.usage_metrics
            token_usage_dict = {
                "total_tokens": getattr(token_usage_metrics, 'total_tokens', 0),
                "prompt_tokens": getattr(token_usage_metrics, 'prompt_tokens', 0),
                "cached_prompt_tokens": getattr(token_usage_metrics, 'cached_prompt_tokens', 0),
                "completion_tokens": getattr(token_usage_metrics, 'completion_tokens', 0),
                "successful_requests": getattr(token_usage_metrics, 'successful_requests', 0)
            }
        print(f"[HTTP] Token usage extracted: {token_usage_dict}")

        # PARSE RESULT
        raw_text = result.raw.strip() if hasattr(result, "raw") else str(result).strip()
        parsed_result = extract_and_parse_json(raw_text)

        # GUARANTEE mandate_id IN RESULT
        parsed_result["mandate_id"] = mandate_id_int

        # ADD TOKEN USAGE TO RESULT
        parsed_result["token_usage"] = token_usage_dict

        # EXTRACT company_details
        company_details = parsed_result.get("company_details", [])
        print(f"[HTTP] Screening complete: {len(company_details)} companies qualified")

        #Extracting the list of Company_ids
        screened_company_id_list=[]


        # SAVE RESULTS TO DATABASE
        if company_details:
            try:
                screenings = await ScreeningRepository.process_agent_output(
                    fund_mandate_id=mandate_id_int,
                    selected_parameters=request.mandate_parameters,
                    company_details=company_details,
                    raw_agent_output=json.dumps(parsed_result)
                )
                print(f"[HTTP] Saved {len(screenings)} screening records to database")
            except Exception as db_error:
                print(f"[HTTP] Database save warning: {db_error}")
        else:
            print("[HTTP] ⚠️ No companies matched screening criteria")


        # Extracting the list of Company_ids from the company_details
        for company in company_details:
            company_id = company.get("id")
            if company_id:
                screened_company_id_list.append(company_id)
                print(f"[HTTP] Extracted Company_id: {company_id}")
            else:
                print(f"[HTTP] Warning: Company_id not found in company details: {company}")

        response = {
            "mandate_id": mandate_id_int,
            #"token_usage": token_usage_dict,
            #"company_details": company_details,
            "company_id": screened_company_id_list
        }

        print(f"[HTTP] Response ready with {len(company_details)} companies\n")
        return response

    except HTTPException:
        raise

    except Exception as e:
        print(f"[HTTP] Error: {str(e)}")
        traceback.print_exc()

        raise HTTPException(
            status_code=500,
            detail=f"Screening failed: {str(e)}"
        )