from typing import Optional, List, Dict, Any
from datetime import datetime
from tortoise.exceptions import DoesNotExist
from tortoise.transactions import in_transaction

from database.models import Sourcing, Company, FundMandate


class SourcingRepository:
    """Repository for sourcing operations (single source of truth for sourcing)."""

    @staticmethod
    async def validate_mandate_exists(fund_mandate_id: Optional[int]) -> Optional[FundMandate]:
        """Validate that a FundMandate exists. Returns FundMandate object or None."""
        if fund_mandate_id is None:
            return None
        try:
            return await FundMandate.get(id=fund_mandate_id)
        except DoesNotExist:
            print(f"⚠️ FundMandate ID {fund_mandate_id} does not exist - returning None")
            return None

    @staticmethod
    async def validate_company_exists(company_id: Optional[int]) -> Optional[Company]:
        """Validate that a Company exists. Returns Company object or None."""
        if company_id is None:
            return None
        try:
            return await Company.get(id=company_id)
        except DoesNotExist:
            print(f"⚠️ Company ID {company_id} does not exist - returning None")
            return None

    @staticmethod
    async def create_or_update_sourcing(
            company_id: int,
            fund_mandate_id: int,
            company_data: Dict[str, Any],
            selected_parameters: Dict[str, Any],
    ) -> Sourcing:
        """
        Create a new Sourcing row (allows same company with different mandates).
        Uses high-performance sequential create() method.
        Returns the Sourcing instance.

        Note: `fund_mandate_id` should point to an existing FundMandate.
        """
        # Validate mandate existence (returns object or None)
        validated_mandate = await SourcingRepository.validate_mandate_exists(fund_mandate_id)
        if validated_mandate is None:
            raise ValueError(
                f"FundMandate id={fund_mandate_id} not found; cannot create Sourcing without a valid mandate.")

        # Always create new - allows same company_id with different mandate_ids
        sourcing = await Sourcing.create(
            company_id=company_id,
            company_data=company_data,
            fund_mandate=validated_mandate,
            selected_parameters=selected_parameters
        )
        print(f" Created Sourcing: id={sourcing.id}, company_id={company_id}, mandate_id={validated_mandate.id}")
        return sourcing

    @staticmethod
    async def upsert_bulk_sourcings(
            fund_mandate_id: int,
            filters: Dict[str, Any],
            companies: List[Company],
    ) -> List[int]:
        """
        Create sourcing entries for multiple companies (sequential creates, no bulk insert).
        Allows same company with different mandates.
        Returns list of company_ids saved.
        """
        saved_ids: List[int] = []
        validated_mandate = await SourcingRepository.validate_mandate_exists(fund_mandate_id)
        if validated_mandate is None:
            raise ValueError(
                f"FundMandate id={fund_mandate_id} not found; cannot upsert sourcings without a valid mandate.")

        # Sequential create - no transaction needed for fast insert
        for c in companies:
            # accept either Company instance or dict-like with id
            company_id = getattr(c, 'id', None) or (c.get('id') if isinstance(c, dict) else None)
            if not company_id:
                continue

            company_data = {
                **(getattr(c, 'attributes', {}) if hasattr(c, 'attributes') else (
                    c.get('attributes', {}) if isinstance(c, dict) else {})),
                "Risks": (getattr(c, 'risks', {}) if hasattr(c, 'risks') else (
                    c.get('risks', {}) if isinstance(c, dict) else {}))
            }

            # Always create new entry - allows same company for different mandates
            s = await Sourcing.create(
                company_id=company_id,
                company_data=company_data,
                fund_mandate=validated_mandate,
                selected_parameters=filters
            )
            saved_ids.append(s.company_id)

        return saved_ids

    @staticmethod
    async def get_sourcing_by_company(company_id: int) -> Optional[Sourcing]:
        try:
            return await Sourcing.get(company_id=company_id, deleted_at__isnull=True)
        except DoesNotExist:
            return None

    @staticmethod
    async def get_sourcings_by_mandate(fund_mandate_id: int) -> List[Sourcing]:
        return await Sourcing.filter(fund_mandate_id=fund_mandate_id, deleted_at__isnull=True).all()

    @staticmethod
    async def fetch_all_sourcings() -> List[Sourcing]:
        return await Sourcing.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def soft_delete_sourcing(company_id: int) -> bool:
        try:
            sourcing = await Sourcing.get(company_id=company_id)
            sourcing.deleted_at = datetime.utcnow()
            await sourcing.save()
            print(f"🗑️ Soft-deleted sourcing company_id={company_id}")
            return True
        except DoesNotExist:
            print(f"⚠️ Sourcing company_id={company_id} not found for deletion")
            return False
