"""Employee Document Vault for RosterIQ.

Stores metadata about employee documents (contracts, tax files, visas, certificates, etc).
Actual file storage is external (S3). This module manages document records, versioning,
expiration tracking, and compliance checking.

Document categories:
- CONTRACT: Employment contracts
- TAX_FILE_DECLARATION: TFN declarations for tax purposes
- SUPER_CHOICE: Superannuation fund choice forms
- VISA_WORK_RIGHTS: Visa and work rights documents
- TRAINING_CERT: Training and professional certificates
- POLICE_CHECK: Police checks
- WORKING_WITH_CHILDREN: Working with Children clearance
- LICENSE: Professional or operational licenses
- OTHER: Miscellaneous documents

Data persisted to SQLite for queries and compliance audits.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("rosteriq.document_vault")


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class DocumentCategory(str, Enum):
    """Types of documents in the vault."""
    CONTRACT = "CONTRACT"
    TAX_FILE_DECLARATION = "TAX_FILE_DECLARATION"
    SUPER_CHOICE = "SUPER_CHOICE"
    VISA_WORK_RIGHTS = "VISA_WORK_RIGHTS"
    TRAINING_CERT = "TRAINING_CERT"
    POLICE_CHECK = "POLICE_CHECK"
    WORKING_WITH_CHILDREN = "WORKING_WITH_CHILDREN"
    LICENSE = "LICENSE"
    OTHER = "OTHER"


class DocumentStatus(str, Enum):
    """Status of a document."""
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"
    EXPIRED = "EXPIRED"


@dataclass
class DocumentRecord:
    """Record of an employee document."""
    id: str
    venue_id: str
    employee_id: str
    category: DocumentCategory
    title: str
    file_reference: str  # External storage key/URL
    file_name: str
    file_size_bytes: int
    mime_type: str
    uploaded_by: str
    uploaded_at: str  # ISO datetime
    status: DocumentStatus = DocumentStatus.ACTIVE
    description: Optional[str] = None
    expires_at: Optional[str] = None  # ISO datetime, optional
    tags: List[str] = field(default_factory=list)
    version: int = 1
    previous_version_id: Optional[str] = None
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "employee_id": self.employee_id,
            "category": self.category.value,
            "title": self.title,
            "file_reference": self.file_reference,
            "file_name": self.file_name,
            "file_size_bytes": self.file_size_bytes,
            "mime_type": self.mime_type,
            "uploaded_by": self.uploaded_by,
            "uploaded_at": self.uploaded_at,
            "status": self.status.value,
            "description": self.description,
            "expires_at": self.expires_at,
            "tags": self.tags,
            "version": self.version,
            "previous_version_id": self.previous_version_id,
            "notes": self.notes,
        }


# ---------------------------------------------------------------------------
# Persistence wiring
# ---------------------------------------------------------------------------


def _get_persistence():
    """Lazy import of persistence module."""
    try:
        from rosteriq import persistence as _p
        return _p
    except ImportError:
        return None


_DOCUMENT_VAULT_SCHEMA = """
CREATE TABLE IF NOT EXISTS document_vault (
    id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    employee_id TEXT NOT NULL,
    category TEXT NOT NULL,
    title TEXT NOT NULL,
    file_reference TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    mime_type TEXT NOT NULL,
    uploaded_by TEXT NOT NULL,
    uploaded_at TEXT NOT NULL,
    status TEXT NOT NULL,
    description TEXT,
    expires_at TEXT,
    tags TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    previous_version_id TEXT,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS ix_doc_venue ON document_vault(venue_id);
CREATE INDEX IF NOT EXISTS ix_doc_employee ON document_vault(employee_id);
CREATE INDEX IF NOT EXISTS ix_doc_category ON document_vault(category);
CREATE INDEX IF NOT EXISTS ix_doc_status ON document_vault(status);
CREATE INDEX IF NOT EXISTS ix_doc_expires ON document_vault(expires_at);
CREATE INDEX IF NOT EXISTS ix_doc_venue_employee ON document_vault(venue_id, employee_id);
"""


def _register_schema_and_callbacks():
    """Register schema and rehydration callback. Deferred until persistence is available."""
    try:
        _p = _get_persistence()
        if _p:
            _p.register_schema("document_vault", _DOCUMENT_VAULT_SCHEMA)
            # Register rehydration callback
            def _rehydrate_on_init():
                store = get_document_vault_store()
                store._rehydrate()
            _p.on_init(_rehydrate_on_init)
    except Exception:
        pass


_register_schema_and_callbacks()


# ---------------------------------------------------------------------------
# Document Vault Store
# ---------------------------------------------------------------------------


class DocumentVaultStore:
    """Thread-safe in-memory store for document records with persistence.

    Persists to SQLite on every state change when persistence is enabled.
    Rehydrates from SQLite on app startup via @_p.on_init callback.
    """

    def __init__(self):
        self._documents: Dict[str, DocumentRecord] = {}
        self._lock = threading.Lock()

    def _persist(self, doc: DocumentRecord) -> None:
        """Persist a document record to SQLite if enabled."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        import json
        row = {
            "id": doc.id,
            "venue_id": doc.venue_id,
            "employee_id": doc.employee_id,
            "category": doc.category.value,
            "title": doc.title,
            "file_reference": doc.file_reference,
            "file_name": doc.file_name,
            "file_size_bytes": doc.file_size_bytes,
            "mime_type": doc.mime_type,
            "uploaded_by": doc.uploaded_by,
            "uploaded_at": doc.uploaded_at,
            "status": doc.status.value,
            "description": doc.description,
            "expires_at": doc.expires_at,
            "tags": json.dumps(doc.tags),
            "version": doc.version,
            "previous_version_id": doc.previous_version_id,
            "notes": doc.notes,
        }
        try:
            _p.upsert("document_vault", row, pk="id")
        except Exception as e:
            logger.warning("Failed to persist document %s: %s", doc.id, e)

    def _rehydrate(self) -> None:
        """Load all documents from SQLite. Called on startup by persistence.on_init."""
        _p = _get_persistence()
        if not _p or not _p.is_persistence_enabled():
            return

        try:
            rows = _p.fetchall("SELECT * FROM document_vault")
            for row in rows:
                doc = self._row_to_document(dict(row))
                self._documents[doc.id] = doc
            logger.info("Rehydrated %d documents from persistence", len(self._documents))
        except Exception as e:
            logger.warning("Failed to rehydrate documents: %s", e)

    @staticmethod
    def _row_to_document(row: Dict[str, Any]) -> DocumentRecord:
        """Reconstruct a DocumentRecord from a DB row."""
        import json

        tags = []
        if row.get("tags"):
            try:
                tags = json.loads(row.get("tags", "[]"))
            except (ValueError, TypeError):
                tags = []

        return DocumentRecord(
            id=row["id"],
            venue_id=row["venue_id"],
            employee_id=row["employee_id"],
            category=DocumentCategory(row.get("category", "OTHER")),
            title=row["title"],
            file_reference=row["file_reference"],
            file_name=row["file_name"],
            file_size_bytes=row.get("file_size_bytes", 0),
            mime_type=row.get("mime_type", "application/octet-stream"),
            uploaded_by=row["uploaded_by"],
            uploaded_at=row["uploaded_at"],
            status=DocumentStatus(row.get("status", "ACTIVE")),
            description=row.get("description"),
            expires_at=row.get("expires_at"),
            tags=tags,
            version=row.get("version", 1),
            previous_version_id=row.get("previous_version_id"),
            notes=row.get("notes"),
        )

    def add_document(self, doc_dict: Dict[str, Any]) -> DocumentRecord:
        """Add a new document record.

        Args:
            doc_dict: Dictionary with document fields (id will be generated if missing)

        Returns:
            DocumentRecord
        """
        if "id" not in doc_dict:
            doc_dict["id"] = f"doc_{uuid.uuid4().hex[:12]}"

        # Convert string category to enum if needed
        if isinstance(doc_dict.get("category"), str):
            doc_dict["category"] = DocumentCategory(doc_dict["category"])

        # Convert string status to enum if needed
        if isinstance(doc_dict.get("status"), str):
            doc_dict["status"] = DocumentStatus(doc_dict["status"])

        # Default values
        if "tags" not in doc_dict:
            doc_dict["tags"] = []
        if "version" not in doc_dict:
            doc_dict["version"] = 1
        if "status" not in doc_dict:
            doc_dict["status"] = DocumentStatus.ACTIVE

        doc = DocumentRecord(**doc_dict)
        with self._lock:
            self._documents[doc.id] = doc
        self._persist(doc)
        return doc

    def get_document(self, doc_id: str) -> Optional[DocumentRecord]:
        """Get a document by ID. Returns None if not found."""
        with self._lock:
            return self._documents.get(doc_id)

    def list_documents(
        self,
        venue_id: str,
        employee_id: Optional[str] = None,
        category: Optional[str] = None,
        status: str = "ACTIVE",
    ) -> List[DocumentRecord]:
        """List documents with optional filters.

        Args:
            venue_id: Venue to filter by
            employee_id: Optional employee filter
            category: Optional category filter
            status: Status to filter by (default "ACTIVE")

        Returns:
            List of DocumentRecord objects
        """
        with self._lock:
            docs = [d for d in self._documents.values() if d.venue_id == venue_id]

            if employee_id:
                docs = [d for d in docs if d.employee_id == employee_id]

            if category:
                if isinstance(category, str):
                    category = DocumentCategory(category)
                docs = [d for d in docs if d.category == category]

            if status:
                if isinstance(status, str):
                    status = DocumentStatus(status)
                docs = [d for d in docs if d.status == status]

            # Sort by uploaded_at descending (most recent first)
            docs.sort(key=lambda d: d.uploaded_at, reverse=True)
            return docs

    def update_document(
        self,
        doc_id: str,
        updates_dict: Dict[str, Any],
    ) -> DocumentRecord:
        """Update a document record.

        Args:
            doc_id: Document ID to update
            updates_dict: Dictionary of fields to update

        Returns:
            Updated DocumentRecord

        Raises:
            ValueError if document not found
        """
        with self._lock:
            doc = self._documents.get(doc_id)
            if not doc:
                raise ValueError(f"Document {doc_id} not found")

            # Update fields
            for key, value in updates_dict.items():
                if hasattr(doc, key):
                    if key == "category" and isinstance(value, str):
                        value = DocumentCategory(value)
                    elif key == "status" and isinstance(value, str):
                        value = DocumentStatus(value)
                    setattr(doc, key, value)

        self._persist(doc)
        return doc

    def archive_document(self, doc_id: str) -> DocumentRecord:
        """Archive a document (set status to ARCHIVED).

        Args:
            doc_id: Document ID to archive

        Returns:
            Updated DocumentRecord

        Raises:
            ValueError if document not found
        """
        return self.update_document(doc_id, {"status": DocumentStatus.ARCHIVED})

    def new_version(
        self,
        doc_id: str,
        new_doc_dict: Dict[str, Any],
    ) -> DocumentRecord:
        """Create a new version of a document, archiving the old one.

        Args:
            doc_id: Document ID to version
            new_doc_dict: Dictionary with new document fields

        Returns:
            New DocumentRecord

        Raises:
            ValueError if original document not found
        """
        with self._lock:
            old_doc = self._documents.get(doc_id)
            if not old_doc:
                raise ValueError(f"Document {doc_id} not found")

            # Archive the old version
            old_doc.status = DocumentStatus.ARCHIVED

        # Create new version - preserve required fields from old doc
        new_id = f"doc_{uuid.uuid4().hex[:12]}"
        new_doc_dict["id"] = new_id
        new_doc_dict["employee_id"] = old_doc.employee_id
        new_doc_dict["venue_id"] = old_doc.venue_id
        new_doc_dict["category"] = old_doc.category
        new_doc_dict["title"] = old_doc.title  # Preserve title unless overridden
        new_doc_dict["version"] = old_doc.version + 1
        new_doc_dict["previous_version_id"] = doc_id

        new_doc = self.add_document(new_doc_dict)
        self._persist(old_doc)  # Persist archived version
        return new_doc

    def get_expiring_documents(
        self,
        venue_id: str,
        days_ahead: int = 30,
    ) -> List[DocumentRecord]:
        """Get documents expiring within N days.

        Args:
            venue_id: Venue to filter by
            days_ahead: Number of days ahead to check (default 30)

        Returns:
            List of expiring DocumentRecord objects sorted by expiry date
        """
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        future = now + timedelta(days=days_ahead)

        with self._lock:
            expiring = []
            for doc in self._documents.values():
                if doc.venue_id != venue_id or doc.status != DocumentStatus.ACTIVE:
                    continue
                if not doc.expires_at:
                    continue

                try:
                    expiry = datetime.fromisoformat(doc.expires_at)
                    if now <= expiry <= future:
                        expiring.append(doc)
                except (ValueError, TypeError):
                    pass

            # Sort by expiry date (soonest first)
            expiring.sort(key=lambda d: d.expires_at)
            return expiring

    def get_employee_compliance(
        self,
        venue_id: str,
        employee_id: str,
    ) -> Dict[str, Any]:
        """Check if employee has required compliance documents.

        Required documents:
        - CONTRACT: Employment contract
        - TAX_FILE_DECLARATION: TFN declaration
        - SUPER_CHOICE: Superannuation choice form

        Args:
            venue_id: Venue to check
            employee_id: Employee to check

        Returns:
            Dictionary with compliance status
        """
        required = [
            DocumentCategory.CONTRACT,
            DocumentCategory.TAX_FILE_DECLARATION,
            DocumentCategory.SUPER_CHOICE,
        ]

        with self._lock:
            employee_docs = [
                d for d in self._documents.values()
                if d.venue_id == venue_id
                and d.employee_id == employee_id
                and d.status == DocumentStatus.ACTIVE
            ]

        held_categories = {d.category for d in employee_docs}
        missing = [cat for cat in required if cat not in held_categories]

        return {
            "employee_id": employee_id,
            "venue_id": venue_id,
            "compliant": len(missing) == 0,
            "missing_documents": [cat.value for cat in missing],
            "held_documents": [cat.value for cat in held_categories],
        }

    def search_documents(
        self,
        venue_id: str,
        query: str,
    ) -> List[DocumentRecord]:
        """Search documents by title and tags.

        Args:
            venue_id: Venue to search in
            query: Search query (matches title and tags case-insensitively)

        Returns:
            List of matching DocumentRecord objects
        """
        query_lower = query.lower()

        with self._lock:
            matches = []
            for doc in self._documents.values():
                if doc.venue_id != venue_id:
                    continue

                # Search title
                if query_lower in doc.title.lower():
                    matches.append(doc)
                    continue

                # Search tags
                if any(query_lower in tag.lower() for tag in doc.tags):
                    matches.append(doc)
                    continue

                # Search description
                if doc.description and query_lower in doc.description.lower():
                    matches.append(doc)

            # Sort by title
            matches.sort(key=lambda d: d.title)
            return matches

    def get_document_history(self, doc_id: str) -> List[DocumentRecord]:
        """Get version history for a document (current + all previous versions).

        Args:
            doc_id: Document ID to get history for

        Returns:
            List of DocumentRecord objects in version order (oldest first)
        """
        with self._lock:
            doc = self._documents.get(doc_id)
            if not doc:
                return []

            # Traverse backwards through version chain
            history = [doc]
            current_id = doc.previous_version_id

            while current_id:
                prev_doc = self._documents.get(current_id)
                if not prev_doc:
                    break
                history.append(prev_doc)
                current_id = prev_doc.previous_version_id

            # Reverse to get oldest first
            history.reverse()
            return history

    def _check_expirations(self) -> None:
        """Mark expired documents (called periodically in production).

        Updates status to EXPIRED for any active documents past their expiry date.
        """
        now = datetime.now(timezone.utc)

        with self._lock:
            for doc in self._documents.values():
                if doc.status != DocumentStatus.ACTIVE or not doc.expires_at:
                    continue

                try:
                    expiry = datetime.fromisoformat(doc.expires_at)
                    if now > expiry:
                        doc.status = DocumentStatus.EXPIRED
                        self._persist(doc)
                except (ValueError, TypeError):
                    pass


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_store: Optional[DocumentVaultStore] = None
_store_lock = threading.Lock()


def get_document_vault_store() -> DocumentVaultStore:
    """Get the module-level document vault store singleton.

    Lazily initializes on first call. Thread-safe.
    """
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = DocumentVaultStore()
    return _store


# Test helper: reset singleton
def _reset_for_tests() -> None:
    """Reset the singleton. Used by tests."""
    global _store
    with _store_lock:
        _store = DocumentVaultStore.__new__(DocumentVaultStore)
        _store._lock = threading.Lock()
        _store._documents = {}
