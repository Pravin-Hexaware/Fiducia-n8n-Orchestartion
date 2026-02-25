from database.models import (
    ExtractedParameters,
    SourcingParameters,
    ScreeningParameters,
    RiskParameters,
)
from tortoise.exceptions import DoesNotExist
from tortoise.transactions import in_transaction
from typing import Optional, List, Dict, Any
from datetime import datetime


class SourcingParametersRepository:
    @staticmethod
    async def create_sourcing_params(key: str, value: str,
                                     extracted_parameters_id: Optional[int] = None) -> SourcingParameters:
        """Create a sourcing parameter key-value pair"""
        return await SourcingParameters.create(
            key=key,
            value=str(value),
            extracted_parameters_id=extracted_parameters_id
        )

    @staticmethod
    async def fetch_by_id(param_id: int) -> Optional[SourcingParameters]:
        """Fetch sourcing parameters by ID"""
        try:
            return await SourcingParameters.get(id=param_id, deleted_at__isnull=True)
        except DoesNotExist:
            return None

    @staticmethod
    async def fetch_all() -> List[SourcingParameters]:
        """Fetch all sourcing parameters"""
        return await SourcingParameters.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def soft_delete(param_id: int) -> bool:
        """Soft delete sourcing parameters"""
        param = await SourcingParametersRepository.fetch_by_id(param_id)
        if not param:
            return False
        param.deleted_at = datetime.utcnow()
        await param.save()
        return True

    @staticmethod
    async def fetch_count() -> int:
        """Get count of sourcing parameters"""
        return await SourcingParameters.filter(deleted_at__isnull=True).count()


class ScreeningParametersRepository:
    @staticmethod
    async def create_screening_params(key: str, value: str,
                                      extracted_parameters_id: Optional[int] = None) -> ScreeningParameters:
        """Create a screening parameter key-value pair"""
        return await ScreeningParameters.create(
            key=key,
            value=str(value),
            extracted_parameters_id=extracted_parameters_id
        )

    @staticmethod
    async def fetch_by_id(param_id: int) -> Optional[ScreeningParameters]:
        """Fetch screening parameters by ID"""
        try:
            return await ScreeningParameters.get(id=param_id, deleted_at__isnull=True)
        except DoesNotExist:
            return None

    @staticmethod
    async def fetch_all() -> List[ScreeningParameters]:
        """Fetch all screening parameters"""
        return await ScreeningParameters.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def soft_delete(param_id: int) -> bool:
        """Soft delete screening parameters"""
        param = await ScreeningParametersRepository.fetch_by_id(param_id)
        if not param:
            return False
        param.deleted_at = datetime.utcnow()
        await param.save()
        return True

    @staticmethod
    async def fetch_count() -> int:
        """Get count of screening parameters"""
        return await ScreeningParameters.filter(deleted_at__isnull=True).count()


class RiskParametersRepository:
    @staticmethod
    async def create_risk_params(key: str, value: str, extracted_parameters_id: Optional[int] = None) -> RiskParameters:
        """Create a risk parameter key-value pair"""
        return await RiskParameters.create(
            key=key,
            value=str(value),
            extracted_parameters_id=extracted_parameters_id
        )

    @staticmethod
    async def fetch_by_id(param_id: int) -> Optional[RiskParameters]:
        """Fetch risk parameters by ID"""
        try:
            return await RiskParameters.get(id=param_id, deleted_at__isnull=True)
        except DoesNotExist:
            return None

    @staticmethod
    async def fetch_all() -> List[RiskParameters]:
        """Fetch all risk parameters"""
        return await RiskParameters.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def fetch_by_extracted_parameters_id(extracted_parameters_id: int) -> List[RiskParameters]:
        """Fetch all risk parameters for a given extracted_parameters_id"""
        return await RiskParameters.filter(
            extracted_parameters_id=extracted_parameters_id,
            deleted_at__isnull=True
        ).all()

    @staticmethod
    async def soft_delete(param_id: int) -> bool:
        """Soft delete risk parameters"""
        param = await RiskParametersRepository.fetch_by_id(param_id)
        if not param:
            return False
        param.deleted_at = datetime.utcnow()
        await param.save()
        return True

    @staticmethod
    async def fetch_count() -> int:
        """Get count of risk parameters"""
        return await RiskParameters.filter(deleted_at__isnull=True).count()


class ExtractedParametersRepository:
    @staticmethod
    async def create_extracted_parameters(
            criteria: Dict[str, Any],
            fund_mandate_id: Optional[int] = None
    ) -> Optional[ExtractedParameters]:
        """
        Create extracted parameters from criteria data.

        Maps subprocesses to parameter types:
        - "Sector & Industry Research" -> SourcingParameters (creates multiple rows for each key-value pair)
        - "Bottom-Up Fundamental Analysis" -> ScreeningParameters (creates multiple rows for each key-value pair)
        - "Risk Assessment of Investment Ideas" -> RiskParameters (creates multiple rows for each key-value pair)

        Args:
            criteria: Dict with structure {'mandate': {...}}
            fund_mandate_id: Optional FK to FundMandate

        Returns:
            ExtractedParameters object or None if no valid criteria
        """
        try:
            # Extract mandate criteria
            mandate = criteria.get('mandate', {})
            if not mandate:
                return None

            # Create ExtractedParameters record first
            async with in_transaction():
                extracted = await ExtractedParameters.create(
                    raw_response=criteria
                )

                # Sourcing: "Sector & Industry Research" - create one row per key-value pair
                if 'Sector & Industry Research' in mandate:
                    sector_industry = mandate['Sector & Industry Research']
                    if sector_industry and isinstance(sector_industry, dict):
                        for key, value in sector_industry.items():
                            await SourcingParametersRepository.create_sourcing_params(
                                key=key,
                                value=value,
                                extracted_parameters_id=extracted.id
                            )

                # Screening: "Bottom-Up Fundamental Analysis" - create one row per key-value pair
                if 'Bottom-Up Fundamental Analysis' in mandate:
                    bottom_up = mandate['Bottom-Up Fundamental Analysis']
                    if bottom_up and isinstance(bottom_up, dict):
                        for key, value in bottom_up.items():
                            await ScreeningParametersRepository.create_screening_params(
                                key=key,
                                value=value,
                                extracted_parameters_id=extracted.id
                            )

                # Risk: "Risk Assessment of Investment Ideas" - create one row per key-value pair
                if 'Risk Assessment of Investment Ideas' in mandate:
                    risk_assess = mandate['Risk Assessment of Investment Ideas']
                    if risk_assess and isinstance(risk_assess, dict):
                        for key, value in risk_assess.items():
                            await RiskParametersRepository.create_risk_params(
                                key=key,
                                value=value,
                                extracted_parameters_id=extracted.id
                            )

            return extracted

        except Exception as e:
            print(f"Error creating extracted parameters: {e}")
            return None

    @staticmethod
    async def fetch_by_id(extracted_id: int) -> Optional[ExtractedParameters]:
        """Fetch extracted parameters by ID"""
        try:
            return await ExtractedParameters.get(id=extracted_id, deleted_at__isnull=True)
        except DoesNotExist:
            return None

    @staticmethod
    async def fetch_all() -> List[ExtractedParameters]:
        """Fetch all extracted parameters"""
        return await ExtractedParameters.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def soft_delete(extracted_id: int) -> bool:
        """Soft delete extracted parameters"""
        extracted = await ExtractedParametersRepository.fetch_by_id(extracted_id)
        if not extracted:
            return False
        extracted.deleted_at = datetime.utcnow()
        await extracted.save()
        return True

    @staticmethod
    async def hard_delete(extracted_id: int) -> bool:
        """Hard delete extracted parameters"""
        try:
            extracted = await ExtractedParameters.get_or_none(id=extracted_id)
            if not extracted:
                return False
            await extracted.delete()
            return True
        except Exception:
            return False
