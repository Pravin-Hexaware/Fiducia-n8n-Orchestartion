from database.models import FundMandate, ExtractedParameters
from tortoise.exceptions import DoesNotExist
from tortoise.transactions import in_transaction
from typing import Optional, List
from datetime import datetime

class FundMandateRepository:
    @staticmethod
    async def create_mandate(
        legal_name: str,
        strategy_type: str,
        vintage_year: int,
        primary_analyst: str,
        processing_date: Optional[str] = None,
        target_count: Optional[int] = None,
        description: Optional[str] = None,
        extracted_parameters_id: Optional[int] = None
    ) -> FundMandate:
        """Create a new fund mandate"""
        # Convert processing_date string to datetime if provided
        processing_date_dt = None
        if processing_date:
            try:
                processing_date_dt = datetime.fromisoformat(processing_date)
            except:
                processing_date_dt = None
        
        mandate = await FundMandate.create(
            legal_name=legal_name,
            strategy_type=strategy_type,
            vintage_year=vintage_year,
            primary_analyst=primary_analyst,
            processing_date=processing_date_dt,
            target_count=target_count,
            description=description,
            extracted_parameters_id=extracted_parameters_id
        )
        return mandate

    @staticmethod
    async def fetch_all_mandate() -> List[FundMandate]:
        """Fetch all non-deleted fund mandates"""
        return await FundMandate.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def fetch_by_id(mandate_id: int) -> Optional[FundMandate]:
        """Fetch a fund mandate by ID"""
        try:
            return await FundMandate.get(id=mandate_id, deleted_at__isnull=True)
        except DoesNotExist:
            return None

    @staticmethod
    async def soft_delete(mandate_id: int) -> bool:
        """Soft delete a fund mandate (set deleted_at timestamp)"""
        mandate = await FundMandateRepository.fetch_by_id(mandate_id)
        if not mandate:
            return False
        mandate.deleted_at = datetime.utcnow()
        await mandate.save()
        return True

    @staticmethod
    async def hard_delete(mandate_id: int) -> bool:
        """Hard delete a fund mandate (permanently remove from database)"""
        mandate = await FundMandate.get_or_none(id=mandate_id)
        if not mandate:
            return False
        await mandate.delete()
        return True

    @staticmethod
    async def update_mandate(
        mandate_id: int,
        fund_name: Optional[str] = None,
        fund_size: Optional[str] = None
    ) -> Optional[FundMandate]:
        """Update fund mandate with new fund name and/or fund size"""
        mandate = await FundMandateRepository.fetch_by_id(mandate_id)
        if not mandate:
            return None

        if fund_name is not None:
            mandate.fund_name = fund_name
        if fund_size is not None:
            mandate.fund_size = fund_size

        await mandate.save()
        return mandate

    @staticmethod
    async def update_extracted_parameters(mandate_id: int, extracted_parameters_id: int) -> Optional[FundMandate]:
        """Link an ExtractedParameters record to a FundMandate"""
        mandate = await FundMandateRepository.fetch_by_id(mandate_id)
        if not mandate:
            return None

        mandate.extracted_parameters_id = extracted_parameters_id
        await mandate.save()
        return mandate

    @staticmethod
    async def update_last_used(mandate_id: int) -> Optional[FundMandate]:
        """Update the last used timestamp by updating updated_at field"""
        mandate = await FundMandateRepository.fetch_by_id(mandate_id)
        if not mandate:
            return None

        mandate.updated_at = datetime.utcnow()
        await mandate.save()
        return mandate