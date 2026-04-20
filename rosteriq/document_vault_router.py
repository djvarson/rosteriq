"""FastAPI router for Document Vault endpoints.

Provides REST API for document management:
- Upload and store document metadata
- List and search documents
- Track document versions
- Check expiration dates
- Verify employee compliance

Auth gating:
- L1 (read): List, get, search, compliance check
- L2 (write): Add, update, archive, version
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.document_vault_router")


def _gate(request: Any, level_name: str) -> None:
    """Auth gate for API endpoints.

    Args:
        request: FastAPI request object
        level_name: Access level name ("L1", "L2", etc)
    """
    try:
        from rosteriq.auth import require_access
        require_access(request, level_name)
    except Exception:
        pass


def create_router():
    """Create and configure the document vault API router.

    Returns:
        FastAPI APIRouter
    """
    try:
        from fastapi import APIRouter, Request, HTTPException, status
        from pydantic import BaseModel
    except ImportError:
        # For development when fastapi/pydantic not installed
        return None

    from rosteriq.document_vault import (
        get_document_vault_store,
        DocumentCategory,
        DocumentStatus,
    )

    router = APIRouter(prefix="/documents", tags=["documents"])

    # -----------------------------------------------------------------------
    # Pydantic models
    # -----------------------------------------------------------------------

    class DocumentUploadRequest(BaseModel):
        venue_id: str
        employee_id: str
        category: str
        title: str
        file_reference: str
        file_name: str
        file_size_bytes: int
        mime_type: str
        uploaded_by: str
        description: Optional[str] = None
        expires_at: Optional[str] = None
        tags: List[str] = []
        notes: Optional[str] = None

    class DocumentUpdateRequest(BaseModel):
        title: Optional[str] = None
        description: Optional[str] = None
        expires_at: Optional[str] = None
        tags: Optional[List[str]] = None
        notes: Optional[str] = None

    class DocumentNewVersionRequest(BaseModel):
        file_reference: str
        file_name: str
        file_size_bytes: int
        mime_type: str
        uploaded_by: str
        description: Optional[str] = None
        expires_at: Optional[str] = None
        tags: Optional[List[str]] = None
        notes: Optional[str] = None

    # -----------------------------------------------------------------------
    # Endpoints
    # -----------------------------------------------------------------------

    @router.post("/", status_code=201)
    def upload_document(request: Request, payload: DocumentUploadRequest) -> Dict[str, Any]:
        """Upload document metadata (L2+).

        Args:
            payload: Document upload request

        Returns:
            Document record as dict
        """
        _gate(request, "L2")

        store = get_document_vault_store()
        doc_dict = {
            "venue_id": payload.venue_id,
            "employee_id": payload.employee_id,
            "category": payload.category,
            "title": payload.title,
            "file_reference": payload.file_reference,
            "file_name": payload.file_name,
            "file_size_bytes": payload.file_size_bytes,
            "mime_type": payload.mime_type,
            "uploaded_by": payload.uploaded_by,
            "uploaded_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "description": payload.description,
            "expires_at": payload.expires_at,
            "tags": payload.tags,
            "notes": payload.notes,
        }

        try:
            doc = store.add_document(doc_dict)
            return doc.to_dict()
        except Exception as e:
            logger.error("Failed to upload document: %s", e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to upload document: {str(e)}",
            )

    @router.get("/{doc_id}")
    def get_document(request: Request, doc_id: str) -> Dict[str, Any]:
        """Get document by ID (L1+).

        Args:
            doc_id: Document ID

        Returns:
            Document record as dict
        """
        _gate(request, "L1")

        store = get_document_vault_store()
        doc = store.get_document(doc_id)

        if not doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Document {doc_id} not found",
            )

        return doc.to_dict()

    @router.get("/venue/{venue_id}")
    def list_venue_documents(
        request: Request,
        venue_id: str,
        employee_id: Optional[str] = None,
        category: Optional[str] = None,
        status: str = "ACTIVE",
    ) -> List[Dict[str, Any]]:
        """List documents for venue (L1+).

        Args:
            venue_id: Venue ID
            employee_id: Optional employee filter
            category: Optional category filter
            status: Status filter (default "ACTIVE")

        Returns:
            List of document records
        """
        _gate(request, "L1")

        store = get_document_vault_store()
        docs = store.list_documents(venue_id, employee_id, category, status)
        return [doc.to_dict() for doc in docs]

    @router.get("/employee/{venue_id}/{employee_id}")
    def list_employee_documents(
        request: Request,
        venue_id: str,
        employee_id: str,
        status: str = "ACTIVE",
    ) -> List[Dict[str, Any]]:
        """List documents for employee (L1+).

        Args:
            venue_id: Venue ID
            employee_id: Employee ID
            status: Status filter (default "ACTIVE")

        Returns:
            List of document records
        """
        _gate(request, "L1")

        store = get_document_vault_store()
        docs = store.list_documents(venue_id, employee_id=employee_id, status=status)
        return [doc.to_dict() for doc in docs]

    @router.put("/{doc_id}")
    def update_document(
        request: Request,
        doc_id: str,
        payload: DocumentUpdateRequest,
    ) -> Dict[str, Any]:
        """Update document metadata (L2+).

        Args:
            doc_id: Document ID
            payload: Update request

        Returns:
            Updated document record as dict
        """
        _gate(request, "L2")

        store = get_document_vault_store()
        updates = {}

        if payload.title is not None:
            updates["title"] = payload.title
        if payload.description is not None:
            updates["description"] = payload.description
        if payload.expires_at is not None:
            updates["expires_at"] = payload.expires_at
        if payload.tags is not None:
            updates["tags"] = payload.tags
        if payload.notes is not None:
            updates["notes"] = payload.notes

        try:
            doc = store.update_document(doc_id, updates)
            return doc.to_dict()
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e),
            )
        except Exception as e:
            logger.error("Failed to update document: %s", e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to update document: {str(e)}",
            )

    @router.post("/{doc_id}/archive")
    def archive_document(request: Request, doc_id: str) -> Dict[str, Any]:
        """Archive document (L2+).

        Args:
            doc_id: Document ID

        Returns:
            Updated document record as dict
        """
        _gate(request, "L2")

        store = get_document_vault_store()

        try:
            doc = store.archive_document(doc_id)
            return doc.to_dict()
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e),
            )
        except Exception as e:
            logger.error("Failed to archive document: %s", e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to archive document: {str(e)}",
            )

    @router.post("/{doc_id}/new-version")
    def create_new_version(
        request: Request,
        doc_id: str,
        payload: DocumentNewVersionRequest,
    ) -> Dict[str, Any]:
        """Create new version of document (L2+).

        Args:
            doc_id: Original document ID
            payload: New version request

        Returns:
            New document record as dict
        """
        _gate(request, "L2")

        store = get_document_vault_store()
        new_doc_dict = {
            "file_reference": payload.file_reference,
            "file_name": payload.file_name,
            "file_size_bytes": payload.file_size_bytes,
            "mime_type": payload.mime_type,
            "uploaded_by": payload.uploaded_by,
            "uploaded_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "description": payload.description,
            "expires_at": payload.expires_at,
            "tags": payload.tags or [],
            "notes": payload.notes,
        }

        try:
            doc = store.new_version(doc_id, new_doc_dict)
            return doc.to_dict()
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e),
            )
        except Exception as e:
            logger.error("Failed to create new version: %s", e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to create new version: {str(e)}",
            )

    @router.get("/expiring/{venue_id}")
    def get_expiring_documents(
        request: Request,
        venue_id: str,
        days_ahead: int = 30,
    ) -> List[Dict[str, Any]]:
        """Get documents expiring within N days (L1+).

        Args:
            venue_id: Venue ID
            days_ahead: Number of days ahead to check (default 30)

        Returns:
            List of expiring document records
        """
        _gate(request, "L1")

        store = get_document_vault_store()
        docs = store.get_expiring_documents(venue_id, days_ahead)
        return [doc.to_dict() for doc in docs]

    @router.get("/compliance/{venue_id}/{employee_id}")
    def check_compliance(
        request: Request,
        venue_id: str,
        employee_id: str,
    ) -> Dict[str, Any]:
        """Check employee compliance (L1+).

        Args:
            venue_id: Venue ID
            employee_id: Employee ID

        Returns:
            Compliance status dict
        """
        _gate(request, "L1")

        store = get_document_vault_store()
        compliance = store.get_employee_compliance(venue_id, employee_id)
        return compliance

    @router.get("/search/{venue_id}")
    def search_documents(
        request: Request,
        venue_id: str,
        q: str,
    ) -> List[Dict[str, Any]]:
        """Search documents by title and tags (L1+).

        Args:
            venue_id: Venue ID
            q: Search query

        Returns:
            List of matching document records
        """
        _gate(request, "L1")

        if not q or len(q.strip()) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Search query cannot be empty",
            )

        store = get_document_vault_store()
        docs = store.search_documents(venue_id, q)
        return [doc.to_dict() for doc in docs]

    @router.get("/history/{doc_id}")
    def get_version_history(request: Request, doc_id: str) -> List[Dict[str, Any]]:
        """Get document version history (L1+).

        Args:
            doc_id: Document ID

        Returns:
            List of document versions (oldest first)
        """
        _gate(request, "L1")

        store = get_document_vault_store()
        docs = store.get_document_history(doc_id)
        return [doc.to_dict() for doc in docs]

    return router


# ============================================================================
# Integration with FastAPI app
# ============================================================================

def register_document_vault_router(app):
    """Register the document vault router with a FastAPI app.

    Args:
        app: FastAPI application instance
    """
    try:
        router = create_router()
        if router:
            app.include_router(router)
            logger.info("Document Vault router registered")
    except Exception as e:
        logger.error("Failed to register Document Vault router: %s", e)
