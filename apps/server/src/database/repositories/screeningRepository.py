from database.models import Screening, Company, FundMandate
from tortoise.exceptions import DoesNotExist
from typing import Optional, List, Dict, Any
from datetime import datetime
import json


class ScreeningRepository:
    """Repository for storing screening results - one record per company"""

    @staticmethod
    async def validate_mandate_exists(fund_mandate_id: Optional[int]) -> Optional[FundMandate]:
        """Validate that mandate exists, return FundMandate object or None"""
        if fund_mandate_id is None:
            return None

        try:
            mandate = await FundMandate.get(id=fund_mandate_id)
            return mandate  # ← Return the OBJECT, not the ID
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
    async def process_agent_output(
            fund_mandate_id: Optional[int],
            selected_parameters: dict,
            company_details: List[Dict[str, Any]],
            raw_agent_output: str
    ) -> List[Screening]:
        """Process agent output and create one screening record per company detail"""
        screenings = []

        # Validate mandate exists, get the OBJECT
        validated_mandate = await ScreeningRepository.validate_mandate_exists(fund_mandate_id)

        for company_data in company_details:
            # Resolve company FK (support both 'Company_id' and 'id' payloads)
            company_id = company_data.get("Company_id") or company_data.get("id")
            company_obj = await ScreeningRepository.validate_company_exists(company_id)

            # Create one screening record for this company (pass company OBJECT or None)
            screening = await Screening.create(
                fund_mandate=validated_mandate,
                company=company_obj,
                selected_parameters=selected_parameters,
                status=company_data.get("status"),
                reason=company_data.get("reason"),
                raw_agent_output=raw_agent_output
            )
            screenings.append(screening)
            mandate_id_display = validated_mandate.id if validated_mandate else 'NULL'
            company_id_display = company_obj.id if company_obj else (company_id or 'NULL')
            print(f"✅ Screening created - Company ID: {company_id_display}, Mandate ID: {mandate_id_display}")

        return screenings

    @staticmethod
    async def create_screening(
            fund_mandate_id: Optional[int] = None,
            company_id: Optional[int] = None,
            selected_parameters: dict = None,
            status: str = None,
            reason: str = None,
            raw_agent_output: str = None
    ) -> Screening:
        """Create a screening record for one company"""
        # Validate mandate if provided - get the OBJECT
        validated_mandate = await ScreeningRepository.validate_mandate_exists(fund_mandate_id)

        # Resolve company object if possible
        company_obj = await ScreeningRepository.validate_company_exists(company_id)

        screening = await Screening.create(
            fund_mandate=validated_mandate,
            company=company_obj,
            selected_parameters=selected_parameters or {},
            status=status,
            reason=reason,
            raw_agent_output=raw_agent_output
        )
        return screening

    @staticmethod
    async def get_screening_by_id(screening_id: int) -> Optional[Screening]:
        """Get a screening record by ID"""
        try:
            return await Screening.get(id=screening_id, deleted_at__isnull=True)
        except DoesNotExist:
            return None

    @staticmethod
    async def get_screenings_by_mandate(fund_mandate_id: int) -> List[Screening]:
        """Get all screening records for a fund mandate"""
        return await Screening.filter(
            fund_mandate_id=fund_mandate_id,
            deleted_at__isnull=True
        ).all()

    @staticmethod
    async def get_screenings_by_company(company_id: int) -> List[Screening]:
        """Get all screening records for a specific company"""
        return await Screening.filter(
            company_id=company_id,
            deleted_at__isnull=True
        ).all()

    @staticmethod
    async def fetch_all_screenings() -> List[Screening]:
        """Get all screening records"""
        return await Screening.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def soft_delete_screening(screening_id: int) -> bool:
        """Soft delete a screening record"""
        try:
            screening = await Screening.get(id=screening_id)
            screening.deleted_at = datetime.utcnow()
            await screening.save()
            return True
        except DoesNotExist:
            return False