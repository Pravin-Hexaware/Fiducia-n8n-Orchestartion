from fastapi import APIRouter, WebSocket, WebSocketDisconnect, File, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from agents.report_agent import create_report_pdf
import json
import queue
import threading
import asyncio
import io
import base64
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, ConfigDict
import os
from pathlib import Path


class ReportGenerationRequest(BaseModel):
    """Request model for report generation - Database-Driven"""
    mandate_id: int  # REQUIRED - ID of the fund mandate to fetch all data from

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mandate_id": 2
            }
        }
    )


router = APIRouter(prefix="/report", tags=["report-generation"])


# ============================================================================
# WEBSOCKET ENDPOINT FOR REAL-TIME REPORT STREAMING
# ============================================================================

@router.websocket("/generate")
async def websocket_generate_report(websocket: WebSocket):
    """
    Real-time WebSocket endpoint for Report Generation with database-driven workflow.

    EXPECTED INPUT FORMAT:
    {
        "mandate_id": 2  # REQUIRED - ID of the fund mandate to fetch all data from
    }

    IMPORTANT NOTES:
    - mandate_id is REQUIRED - all data (sourcing, screening, risk_analysis) is fetched from database
    - The endpoint fetches:
      * Mandate details from FundMandate table
      * Sourced companies from Sourcing table (or derived from Screening/RiskAnalysis if needed)
      * Screened companies from Screening table
      * Risk analysis results from RiskAnalysis table
    - All parameter_analysis data comes from the database RiskAnalysis table

    STREAMING EVENTS (Real-time):
    - session_start: Report generation initialized
    - report_progress: Fetching data from database
    - tool_result: Tool execution progress (fetch_mandate_data, analyze_and_generate_report, generate_pdf_report)
    - report_data: Final structured report data
    - pdf_data: Base64-encoded PDF binary
    - error: If generation fails

    WORKFLOW:
    1. Client connects to ws://server:8000/report/generate
    2. Client sends JSON with mandate_id (REQUIRED)
    3. Server fetches all data from database using mandate_id:
       - Mandate details from FundMandate table
       - Sourced companies from Sourcing table
       - Screened companies from Screening table
       - Risk analysis from RiskAnalysis table
    4. Server streams progress events in real-time
    5. LLM generates executive summary, key findings, critical risks
    6. PDF is generated with mandate details and all analysis
    7. PDF and report JSON are streamed back as base64
    8. Session closes with completion status
    """
    await websocket.accept()

    try:
        data_json = await websocket.receive_text()
        data = ReportGenerationRequest(**json.loads(data_json))

        if not data.mandate_id:
            await websocket.send_json({
                "type": "error",
                "message": "mandate_id is REQUIRED for report generation",
                "timestamp": datetime.now().isoformat()
            })
            await websocket.close()
            return

        event_queue = queue.Queue()

        def run_report_generation_thread():
            """Runs report generation in background thread to allow async streaming"""
            try:
                file_path, pdf_bytes, report_text = create_report_pdf(
                    risk_results=None,  # Not used - data comes from database
                    output_path="./reports/report.pdf",
                    event_queue=event_queue,
                    mandate_id=data.mandate_id  # REQUIRED - fetches all data from database
                )

                # Send final PDF data
                pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
                event_queue.put({
                    "type": "pdf_data",
                    "pdf_base64": pdf_base64,
                    "file_path": file_path,
                    "pdf_size_bytes": len(pdf_bytes),
                    "timestamp": datetime.now().isoformat()
                })

                # Send end signal
                event_queue.put(None)

            except Exception as e:
                event_queue.put({
                    "type": "error",
                    "message": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                event_queue.put(None)

        generation_thread = threading.Thread(target=run_report_generation_thread, daemon=True)
        generation_thread.start()

        print(f"Starting real-time report generation streaming for mandate {data.mandate_id}...")
        while True:
            try:
                event = event_queue.get(timeout=0.1)

                if event is None:
                    print("Report generation stream complete")
                    break

                await websocket.send_json(event)
                print(f"Streamed: {event.get('type')}")

                await asyncio.sleep(0.02)

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error sending event: {e}")
                break

    except WebSocketDisconnect:
        print("Client disconnected from report generation")
    except json.JSONDecodeError as e:
        try:
            await websocket.send_json({
                "type": "error",
                "message": f"Invalid JSON: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })
        except:
            pass
        await websocket.close()
    except Exception as e:
        print(f"WebSocket error: {str(e)}")
        try:
            await websocket.send_json({
                "type": "error",
                "message": f"Server error: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })
        except:
            pass
        await websocket.close()


# ============================================================================
# HTTP ENDPOINT - GENERATE REPORT (Database-Driven - mandate_id only)
# ============================================================================

@router.post("/generate-http")
async def http_generate_report(request: ReportGenerationRequest):
    """
    HTTP POST endpoint for report generation - Database-Driven with mandate_id ONLY.

    This endpoint fetches ALL data from three database tables using mandate_id:
    - Sourcing table → Sourced companies
    - Screening table → Screened companies
    - RiskAnalysis table → Risk assessment results

    REQUIRED INPUT:
    {
        "mandate_id": 2
    }

    That's it! No need to pass risk_results separately - they're already in the database.

    RESPONSE FORMAT:
    {
        "status": "success",
        "timestamp": "...",
        "report": {
            "generated_at": "...",
            "total_companies": 5,
            "safe_companies": 3,
            "unsafe_companies": 2,
            "success_rate": 60.0,
            "company_summaries": [...],
            "executive_summary": "...",
            "key_findings": [...],
            "critical_risks": [...]
        },
        "pdf_base64": "base64_encoded_pdf_bytes",
        "file_path": "path_to_generated_pdf",
        "pdf_size_bytes": 12345
    }
    """
    try:
        print("\n" + "=" * 80)
        print("🟢 HTTP REQUEST RECEIVED - REPORT GENERATION (mandate_id only)")
        print("=" * 80)

        mandate_id = request.mandate_id
        print(f"\n✅ Mandate ID: {mandate_id}")
        print(f"   Fetching data from: Sourcing, Screening, RiskAnalysis tables\n")

        event_queue = queue.Queue()
        all_events = []

        def run_report_generation_thread():
            """Runs report generation in background thread, fetching from database"""
            try:
                print(f"[THREAD] Starting report generation with mandate_id={mandate_id}")

                # Create report by fetching from database using mandate_id
                file_path, pdf_bytes, report_text = create_report_pdf(
                    risk_results=None,  # Not used
                    output_path="./reports/report.pdf",
                    event_queue=event_queue,
                    mandate_id=mandate_id  # Only parameter needed
                )

                print(f"[THREAD] Report generated successfully")
                print(f"[THREAD] PDF size: {len(pdf_bytes)} bytes")

                # Send final PDF data
                pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
                event_queue.put({
                    "type": "pdf_data",
                    "pdf_base64": pdf_base64,
                    "file_path": file_path,
                    "pdf_size_bytes": len(pdf_bytes),
                    "timestamp": datetime.now().isoformat()
                })

                # Send end signal
                event_queue.put(None)

            except Exception as e:
                print(f"[THREAD ERROR] {str(e)}")
                import traceback
                traceback.print_exc()
                event_queue.put({
                    "type": "error",
                    "message": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                event_queue.put(None)

        generation_thread = threading.Thread(target=run_report_generation_thread, daemon=True)
        generation_thread.start()

        print("Collecting all events from report generation...")
        pdf_data = None
        report_data = None
        while True:
            try:
                event = event_queue.get(timeout=0.5)

                if event is None:
                    print("Report generation complete")
                    break

                all_events.append(event)
                print(f"Collected: {event.get('type')}")

                # Store PDF data event
                if event.get("type") == "pdf_data":
                    pdf_data = event

                # Store report data event
                if event.get("type") == "report_data":
                    report_data = event.get("data", {})

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error collecting event: {e}")
                break

        # Wait for thread to complete
        generation_thread.join(timeout=10)

        print("✅ HTTP REQUEST COMPLETED SUCCESSFULLY\n")

        if pdf_data is None:
            # Check if there was an error event
            error_events = [e for e in all_events if e.get("type") == "error"]
            if error_events:
                return {
                    "status": "error",
                    "message": error_events[0].get("message", "Unknown error during report generation"),
                    "timestamp": datetime.now().isoformat()
                }

        response = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
        }

        if report_data:
            response["report"] = report_data

        if pdf_data:
            response.update({
                "pdf_base64": pdf_data.get("pdf_base64"),
                "file_path": pdf_data.get("file_path"),
                "pdf_size_bytes": pdf_data.get("pdf_size_bytes")
            })

        return response

    except Exception as e:
        print(f"[HTTP ERROR] Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }


# ============================================================================
# HTTP ENDPOINT - GENERATE REPORT AND STREAM PDF
# ============================================================================

@router.post("/generate-stream")
async def http_generate_report_stream(request: ReportGenerationRequest):
    """
    HTTP POST endpoint to generate report and stream PDF directly (database-driven).

    Useful for downloading PDF without saving to disk.

    Fetches all data from database using mandate_id and streams PDF back.

    Returns:
    - Content-Type: application/pdf
    - Content-Disposition: attachment
    - Streaming PDF bytes
    """
    try:
        if not request.mandate_id:
            return {
                "status": "error",
                "message": "mandate_id is REQUIRED",
                "timestamp": datetime.now().isoformat()
            }

        file_path, pdf_bytes, report_text = await asyncio.to_thread(
            create_report_pdf,
            risk_results=None,  # Not used - data comes from database
            output_path=None,  # Don't save to disk
            event_queue=None,
            mandate_id=request.mandate_id  # REQUIRED - fetches all data from database
        )

        return StreamingResponse(
            iter([pdf_bytes]),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"attachment; filename=risk_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            }
        )

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }


# ============================================================================
# HTTP ENDPOINT - HEALTH CHECK
# ============================================================================

@router.get("/health")
async def health_check():
    """
    Health check endpoint for report generation service.

    Returns:
    {
        "status": "healthy",
        "service": "report-generation",
        "timestamp": "..."
    }
    """
    return {
        "status": "healthy",
        "service": "report-generation",
        "timestamp": datetime.now().isoformat()
    }


# ============================================================================
# HTTP ENDPOINT - SERVE GENERATED REPORT FILES
# ============================================================================


@router.get('/files/{filename}')
async def serve_report_file(filename: str):
    """Serve a generated report PDF from the reports directory by filename.

    This prevents exposing arbitrary filesystem paths to the frontend. The server
    only serves files located under the `reports` directory next to this module.
    """
    try:
        reports_dir = Path(__file__).parent.parent / 'reports'
        file_path = (reports_dir / filename).resolve()

        # Ensure the requested file is inside the reports directory
        if not str(file_path).startswith(str(reports_dir.resolve())):
            return {"status": "error", "message": "Invalid file path"}

        if not file_path.exists():
            return {"status": "error", "message": "File not found"}

        return FileResponse(path=str(file_path), media_type='application/pdf', filename=filename)
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ============================================================================
# HTTP ENDPOINT - TEST ENDPOINT WITH SAMPLE DATA
# ============================================================================

@router.post("/test-generate")
async def test_generate_report():
    """
    Test endpoint with hardcoded sample risk assessment results.

    No request body needed. Returns sample PDF report.
    Useful for testing the report generation pipeline.

    Includes 3 sample companies with different assessment statuses.
    """
    sample_results = [
        {
            "company_name": "TechCorp Inc",
            "parameter_analysis": {
                "Competitive Position": {
                    "status": "SAFE",
                    "reason": "Strong market leadership position"
                },
                "Governance Quality": {
                    "status": "SAFE",
                    "reason": "Excellent board composition"
                },
                "Customer Concentration Risk": {
                    "status": "UNSAFE",
                    "reason": "70% revenue from top customer"
                }
            },
            "overall_assessment": {
                "status": "UNSAFE",
                "reason": "Customer concentration exceeds mandate threshold"
            }
        },
        {
            "company_name": "FinServe Solutions",
            "parameter_analysis": {
                "Competitive Position": {
                    "status": "SAFE",
                    "reason": "Competitive market position established"
                },
                "Governance Quality": {
                    "status": "SAFE",
                    "reason": "Strong governance framework"
                },
                "Regulatory Compliance": {
                    "status": "SAFE",
                    "reason": "Full regulatory compliance achieved"
                }
            },
            "overall_assessment": {
                "status": "SAFE",
                "reason": "All mandate requirements satisfied"
            }
        },
        {
            "company_name": "GreenEnergy Ltd",
            "parameter_analysis": {
                "Business Model Complexity": {
                    "status": "UNSAFE",
                    "reason": "Overly complex business model"
                },
                "Vendor Dependency": {
                    "status": "UNSAFE",
                    "reason": "Single vendor platform dependency"
                }
            },
            "overall_assessment": {
                "status": "UNSAFE",
                "reason": "Multiple mandate violations detected"
            }
        }
    ]

    try:
        file_path, pdf_bytes, report_text = await asyncio.to_thread(
            create_report_pdf,
            sample_results,
            "./reports/test_report.pdf"
        )

        return FileResponse(
            path=file_path,
            media_type="application/pdf",
            filename=f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        )

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }
