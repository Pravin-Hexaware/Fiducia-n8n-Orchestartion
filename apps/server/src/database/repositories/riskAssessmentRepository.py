from database.models import RiskAnalysis, FundMandate, Company
from tortoise.exceptions import DoesNotExist
from typing import Optional, List, Dict, Any
from datetime import datetime
import json


class RiskAssessmentRepository:
    """Repository for storing risk assessment results - one record per company"""

    @staticmethod
    async def validate_mandate_exists(fund_mandate_id: Optional[int]) -> Optional[FundMandate]:
        """Validate that mandate exists, return FundMandate object or None"""
        if fund_mandate_id is None:
            return None

        try:
            mandate = await FundMandate.get(id=fund_mandate_id)
            return mandate
        except DoesNotExist:
            print(f"⚠️ FundMandate ID {fund_mandate_id} does not exist - storing as NULL")
            return None

    @staticmethod
    async def validate_company_exists(company_id: Optional[int]) -> Optional[Company]:
        """Validate that a Company exists, return Company object or None"""
        if company_id is None:
            return None

        try:
            company = await Company.get(id=company_id)
            return company
        except DoesNotExist:
            print(f"⚠️ Company ID {company_id} does not exist - storing as NULL")
            return None

    @staticmethod
    async def save_assessment_result(
            fund_mandate_id: Optional[int],
            company_id: Optional[int],
            company_name: str,
            parameter_analysis: Dict[str, Any],
            overall_assessment: Dict[str, str]
        ) -> RiskAnalysis:
        """
        Save a company's risk assessment result to database.

        Args:
            fund_mandate_id: ID of the fund mandate
            company_id: ID of the company
            company_name: Name of the company
            parameter_analysis: Parameter-level analysis results
            overall_assessment: Overall assessment with status and reason

        Returns:
            RiskAnalysis model instance
        """
        try:
            print(f"[DB REPO] Saving assessment for: {company_name}")
            print(f"[DB REPO] fund_mandate_id={fund_mandate_id}, company_id={company_id}")


            # Validate mandate exists and resolve company object
            validated_mandate = await RiskAssessmentRepository.validate_mandate_exists(fund_mandate_id)
            company_obj = await RiskAssessmentRepository.validate_company_exists(company_id)

            # Create risk analysis record (pass FK objects where possible)
            print(f"[DB REPO] Creating RiskAnalysis record...")
            result = await RiskAnalysis.create(
                fund_mandate=validated_mandate,
                company=company_obj,
                overall_result=overall_assessment.get('status', 'UNKNOWN'),
                parameter_analysis=parameter_analysis,
                overall_assessment=overall_assessment
            )

            print(f"[DB REPO] ✓ Saved result: ID={result.id}, company={company_name}, status={result.overall_result}")
            return result

        except Exception as e:
            print(f"[DB REPO ERROR] Error saving assessment result for {company_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    @staticmethod
    async def get_results_by_mandate(fund_mandate_id: int) -> List[RiskAnalysis]:
        """Get all results for a fund mandate."""
        try:
            return await RiskAnalysis.filter(fund_mandate_id=fund_mandate_id).all()
        except Exception as e:
            print(f"[DB ERROR] Error fetching results: {str(e)}")
            raise

    @staticmethod
    async def get_results_by_company(company_id: int) -> List[RiskAnalysis]:
        """Get all results for a company."""
        try:
            return await RiskAnalysis.filter(company_id=company_id).all()
        except Exception as e:
            print(f"[DB ERROR] Error fetching results: {str(e)}")
            raise

    @staticmethod
    async def fetch_all_results() -> List[RiskAnalysis]:
        """Get all risk analysis records."""
        try:
            return await RiskAnalysis.filter(deleted_at__isnull=True).all()
        except Exception as e:
            print(f"[DB ERROR] Error fetching all results: {str(e)}")
            raise
