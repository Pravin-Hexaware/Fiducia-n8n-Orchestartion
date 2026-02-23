from tortoise.exceptions import DoesNotExist
from typing import Optional, List
from datetime import datetime
from database.models import GeneratedDocument


class GeneratedDocumentRepository:
    @staticmethod
    async def create_document(
        fund_mandate_id: int,
        generated_content: str
    ) -> Optional[GeneratedDocument]:
        """Create a new generated document"""
        try:
            document = await GeneratedDocument.create(
                fund_mandate_id=fund_mandate_id,
                generated_content=generated_content
            )
            return document
        except Exception as e:
            print(f"Error creating generated document: {e}")
            return None

    @staticmethod
    async def fetch_all() -> List[GeneratedDocument]:
        """Fetch all non-deleted generated documents"""
        return await GeneratedDocument.filter(deleted_at__isnull=True).all()

    @staticmethod
    async def fetch_by_id(document_id: int) -> Optional[GeneratedDocument]:
        """Fetch a generated document by ID"""
        try:
            return await GeneratedDocument.get(id=document_id, deleted_at__isnull=True)
        except DoesNotExist:
            return None

    @staticmethod
    async def fetch_by_mandate_id(fund_mandate_id: int) -> List[GeneratedDocument]:
        """Fetch all generated documents for a specific mandate"""
        return await GeneratedDocument.filter(
            fund_mandate_id=fund_mandate_id,
            deleted_at__isnull=True
        ).all()

    @staticmethod
    async def fetch_count() -> int:
        """Get the total count of generated documents"""
        return await GeneratedDocument.filter(deleted_at__isnull=True).count()

    @staticmethod
    async def soft_delete(document_id: int) -> bool:
        """Soft delete a generated document"""
        document = await GeneratedDocumentRepository.fetch_by_id(document_id)
        if not document:
            return False
        document.deleted_at = datetime.utcnow()
        await document.save()
        return True

    @staticmethod
    async def hard_delete(document_id: int) -> bool:
        """Hard delete a generated document"""
        document = await GeneratedDocument.get_or_none(id=document_id)
        if not document:
            return False
        await document.delete()
        return True

    @staticmethod
    async def update_content(document_id: int, generated_content: str) -> Optional[GeneratedDocument]:
        """Update the content of a generated document"""
        document = await GeneratedDocumentRepository.fetch_by_id(document_id)
        if not document:
            return None
        
        document.generated_content = generated_content
        await document.save()
        return document
