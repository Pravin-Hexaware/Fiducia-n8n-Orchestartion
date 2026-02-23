from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from database.repositories.fundRepository import FundMandateRepository
from database.repositories.companyRepository import CompanyRepository
from database.repositories.GeneratedDocumentRepository import GeneratedDocumentRepository
from database.repositories.ParametersRepository import (
    SourcingParametersRepository,
    ScreeningParametersRepository,
    RiskParametersRepository
)
from datetime import datetime

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

class StatCard(BaseModel):
    label: str
    value: int
    icon: str
    bgColor: str

class MandateRowData(BaseModel):
    id: int
    legal_name: str
    strategy_type: str
    vintage_year: int
    primary_analyst: str
    created_at: str

class DashboardStats(BaseModel):
    fund_mandates: int
    extracted_parameters: int
    companies: int
    generated_documents: int
    recent_mandates: List[MandateRowData]

@router.get("/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    """Get dashboard statistics: mandate count, extracted parameters count, companies count, generated documents count, and top 3 recent mandates"""
    try:
        # Get fund mandates count
        all_mandates = await FundMandateRepository.fetch_all_mandate()
        mandate_count = len(all_mandates)

        # Get extracted parameters count (sum of sourcing, screening, and risk parameters)
        sourcing_count = await SourcingParametersRepository.fetch_count()
        screening_count = await ScreeningParametersRepository.fetch_count()
        risk_count = await RiskParametersRepository.fetch_count()
        extracted_params_count = sourcing_count + screening_count + risk_count

        # Get companies count
        company_count = await CompanyRepository.fetch_count()

        # Get generated documents count
        generated_docs_count = await GeneratedDocumentRepository.fetch_count()

        # Get top 3 recent mandates (sorted by created_at descending)
        recent_mandates = sorted(
            all_mandates,
            key=lambda m: m.created_at if m.created_at else datetime.min,
            reverse=True
        )[:3]

        # Format recent mandates
        formatted_mandates = [
            MandateRowData(
                id=mandate.id,
                legal_name=mandate.legal_name,
                strategy_type=mandate.strategy_type,
                vintage_year=mandate.vintage_year,
                primary_analyst=mandate.primary_analyst,
                created_at=mandate.created_at.isoformat() if mandate.created_at else ""
            )
            for mandate in recent_mandates
        ]

        return DashboardStats(
            fund_mandates=mandate_count,
            extracted_parameters=extracted_params_count,
            companies=company_count,
            generated_documents=generated_docs_count,
            recent_mandates=formatted_mandates
        )

    except Exception as e:
        print(f"Error fetching dashboard stats: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")
