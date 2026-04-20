"""Tests for rosteriq.document_vault — pure-stdlib, no pytest.

Runs with `PYTHONPATH=. python3 -m unittest tests.test_document_vault -v`
Tests cover document CRUD, versioning, expiration tracking, compliance checks,
search, history traversal, and edge cases.
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.document_vault import (  # noqa: E402
    DocumentRecord,
    DocumentCategory,
    DocumentStatus,
    DocumentVaultStore,
    get_document_vault_store,
    _reset_for_tests,
)


def _reset():
    """Reset the store singleton for tests."""
    _reset_for_tests()


def _now_iso():
    """Get current time in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def _future_iso(days=30):
    """Get future time in ISO format."""
    future = datetime.now(timezone.utc) + timedelta(days=days)
    return future.isoformat()


def _past_iso(days=30):
    """Get past time in ISO format."""
    past = datetime.now(timezone.utc) - timedelta(days=days)
    return past.isoformat()


# ============================================================================
# Tests: Document Record Creation
# ============================================================================


class TestDocumentRecordBasics(unittest.TestCase):
    """Tests for basic DocumentRecord functionality."""

    def test_document_record_initialization(self):
        """DocumentRecord can be initialized with required fields."""
        doc = DocumentRecord(
            id="doc_1",
            venue_id="venue_1",
            employee_id="emp_1",
            category=DocumentCategory.CONTRACT,
            title="Employment Contract",
            file_reference="s3://bucket/contract.pdf",
            file_name="contract.pdf",
            file_size_bytes=102400,
            mime_type="application/pdf",
            uploaded_by="hr_user",
            uploaded_at=_now_iso(),
        )
        self.assertEqual(doc.id, "doc_1")
        self.assertEqual(doc.title, "Employment Contract")
        self.assertEqual(doc.status, DocumentStatus.ACTIVE)

    def test_document_record_with_optional_fields(self):
        """DocumentRecord can include optional fields."""
        doc = DocumentRecord(
            id="doc_1",
            venue_id="venue_1",
            employee_id="emp_1",
            category=DocumentCategory.VISA_WORK_RIGHTS,
            title="Work Visa",
            file_reference="s3://bucket/visa.pdf",
            file_name="visa.pdf",
            file_size_bytes=51200,
            mime_type="application/pdf",
            uploaded_by="hr_user",
            uploaded_at=_now_iso(),
            description="Employee work visa",
            expires_at=_future_iso(days=365),
            tags=["visa", "work_rights"],
            notes="Expires next year",
        )
        self.assertEqual(doc.description, "Employee work visa")
        self.assertEqual(doc.tags, ["visa", "work_rights"])
        self.assertIsNotNone(doc.expires_at)

    def test_document_to_dict(self):
        """DocumentRecord.to_dict() returns complete dict."""
        doc = DocumentRecord(
            id="doc_1",
            venue_id="venue_1",
            employee_id="emp_1",
            category=DocumentCategory.CONTRACT,
            title="Contract",
            file_reference="s3://bucket/contract.pdf",
            file_name="contract.pdf",
            file_size_bytes=102400,
            mime_type="application/pdf",
            uploaded_by="hr_user",
            uploaded_at=_now_iso(),
            tags=["contract"],
        )
        d = doc.to_dict()
        self.assertEqual(d["id"], "doc_1")
        self.assertEqual(d["category"], "CONTRACT")
        self.assertEqual(d["status"], "ACTIVE")
        self.assertEqual(d["tags"], ["contract"])


# ============================================================================
# Tests: Store Basic Operations
# ============================================================================


class TestDocumentVaultStoreBasics(unittest.TestCase):
    """Tests for basic store operations."""

    def setUp(self):
        _reset()
        self.store = get_document_vault_store()

    def test_store_singleton(self):
        """Store is a singleton."""
        store1 = get_document_vault_store()
        store2 = get_document_vault_store()
        self.assertIs(store1, store2)

    def test_add_document(self):
        """add_document stores a document."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Employment Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        doc = self.store.add_document(doc_dict)
        self.assertIsNotNone(doc.id)
        self.assertEqual(doc.title, "Employment Contract")
        self.assertEqual(doc.version, 1)

    def test_add_document_generates_id(self):
        """add_document generates ID if not provided."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        doc = self.store.add_document(doc_dict)
        self.assertTrue(doc.id.startswith("doc_"))

    def test_get_document(self):
        """get_document retrieves a document."""
        doc_dict = {
            "id": "doc_1",
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        self.store.add_document(doc_dict)
        doc = self.store.get_document("doc_1")
        self.assertIsNotNone(doc)
        self.assertEqual(doc.title, "Contract")

    def test_get_document_not_found(self):
        """get_document returns None if not found."""
        doc = self.store.get_document("nonexistent")
        self.assertIsNone(doc)

    def test_update_document(self):
        """update_document modifies a document."""
        doc_dict = {
            "id": "doc_1",
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Original Title",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        self.store.add_document(doc_dict)
        updated = self.store.update_document("doc_1", {"title": "Updated Title"})
        self.assertEqual(updated.title, "Updated Title")
        self.assertEqual(self.store.get_document("doc_1").title, "Updated Title")

    def test_update_document_not_found(self):
        """update_document raises ValueError if document not found."""
        with self.assertRaises(ValueError):
            self.store.update_document("nonexistent", {"title": "New Title"})

    def test_archive_document(self):
        """archive_document sets status to ARCHIVED."""
        doc_dict = {
            "id": "doc_1",
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        self.store.add_document(doc_dict)
        archived = self.store.archive_document("doc_1")
        self.assertEqual(archived.status, DocumentStatus.ARCHIVED)


# ============================================================================
# Tests: List and Filter Operations
# ============================================================================


class TestDocumentVaultListAndFilter(unittest.TestCase):
    """Tests for listing and filtering documents."""

    def setUp(self):
        _reset()
        self.store = get_document_vault_store()

        # Add multiple documents
        for i in range(3):
            doc_dict = {
                "venue_id": "venue_1",
                "employee_id": f"emp_{i}",
                "category": "CONTRACT",
                "title": f"Contract {i}",
                "file_reference": f"s3://bucket/contract_{i}.pdf",
                "file_name": f"contract_{i}.pdf",
                "file_size_bytes": 102400,
                "mime_type": "application/pdf",
                "uploaded_by": "hr_user",
                "uploaded_at": _now_iso(),
            }
            self.store.add_document(doc_dict)

        # Add a tax doc for emp_0
        tax_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_0",
            "category": "TAX_FILE_DECLARATION",
            "title": "Tax File Number",
            "file_reference": "s3://bucket/tfn.pdf",
            "file_name": "tfn.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        self.store.add_document(tax_dict)

    def test_list_documents_by_venue(self):
        """list_documents returns all documents for a venue."""
        docs = self.store.list_documents("venue_1")
        self.assertEqual(len(docs), 4)

    def test_list_documents_by_venue_and_employee(self):
        """list_documents filters by employee."""
        docs = self.store.list_documents("venue_1", employee_id="emp_0")
        self.assertEqual(len(docs), 2)

    def test_list_documents_by_category(self):
        """list_documents filters by category."""
        docs = self.store.list_documents(
            "venue_1", category=DocumentCategory.CONTRACT
        )
        self.assertEqual(len(docs), 3)

        tax_docs = self.store.list_documents(
            "venue_1", category=DocumentCategory.TAX_FILE_DECLARATION
        )
        self.assertEqual(len(tax_docs), 1)

    def test_list_documents_by_status(self):
        """list_documents filters by status."""
        # Add an archived document
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_archive",
            "category": "CONTRACT",
            "title": "Archived Contract",
            "file_reference": "s3://bucket/archived.pdf",
            "file_name": "archived.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "status": DocumentStatus.ARCHIVED,
        }
        self.store.add_document(doc_dict)

        active = self.store.list_documents("venue_1", status="ACTIVE")
        self.assertEqual(len(active), 4)

        archived = self.store.list_documents("venue_1", status="ARCHIVED")
        self.assertEqual(len(archived), 1)

    def test_list_documents_empty_result(self):
        """list_documents returns empty list for non-existent venue."""
        docs = self.store.list_documents("nonexistent_venue")
        self.assertEqual(docs, [])

    def test_list_documents_sorted_by_uploaded_at(self):
        """list_documents returns documents sorted by uploaded_at descending."""
        docs = self.store.list_documents("venue_1")
        # Most recent first
        self.assertEqual(len(docs), 4)


# ============================================================================
# Tests: Document Versioning
# ============================================================================


class TestDocumentVaultVersioning(unittest.TestCase):
    """Tests for document versioning."""

    def setUp(self):
        _reset()
        self.store = get_document_vault_store()

    def test_new_version_creates_new_document(self):
        """new_version creates a new document and archives the old one."""
        # Create original
        orig_dict = {
            "id": "doc_v1",
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract v1",
            "file_reference": "s3://bucket/contract_v1.pdf",
            "file_name": "contract_v1.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        orig = self.store.add_document(orig_dict)

        # Create new version
        new_dict = {
            "file_reference": "s3://bucket/contract_v2.pdf",
            "file_name": "contract_v2.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        new_doc = self.store.new_version("doc_v1", new_dict)

        self.assertNotEqual(new_doc.id, orig.id)
        self.assertEqual(new_doc.version, 2)
        self.assertEqual(new_doc.previous_version_id, "doc_v1")

        # Original should be archived
        orig_updated = self.store.get_document("doc_v1")
        self.assertEqual(orig_updated.status, DocumentStatus.ARCHIVED)

    def test_new_version_not_found(self):
        """new_version raises ValueError if original document not found."""
        with self.assertRaises(ValueError):
            self.store.new_version("nonexistent", {})

    def test_get_document_history(self):
        """get_document_history returns version chain."""
        # Create v1
        v1_dict = {
            "id": "doc_v1",
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract_v1.pdf",
            "file_name": "contract_v1.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        v1 = self.store.add_document(v1_dict)

        # Create v2
        v2_dict = {
            "file_reference": "s3://bucket/contract_v2.pdf",
            "file_name": "contract_v2.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        v2 = self.store.new_version(v1.id, v2_dict)

        # Create v3
        v3_dict = {
            "file_reference": "s3://bucket/contract_v3.pdf",
            "file_name": "contract_v3.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        v3 = self.store.new_version(v2.id, v3_dict)

        # Get history from latest
        history = self.store.get_document_history(v3.id)
        self.assertEqual(len(history), 3)
        self.assertEqual(history[0].version, 1)
        self.assertEqual(history[1].version, 2)
        self.assertEqual(history[2].version, 3)

    def test_get_document_history_single_version(self):
        """get_document_history with single version returns list of one."""
        doc_dict = {
            "id": "doc_1",
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        doc = self.store.add_document(doc_dict)
        history = self.store.get_document_history(doc.id)
        self.assertEqual(len(history), 1)

    def test_get_document_history_not_found(self):
        """get_document_history returns empty list if document not found."""
        history = self.store.get_document_history("nonexistent")
        self.assertEqual(history, [])


# ============================================================================
# Tests: Expiration Tracking
# ============================================================================


class TestDocumentVaultExpiration(unittest.TestCase):
    """Tests for document expiration tracking."""

    def setUp(self):
        _reset()
        self.store = get_document_vault_store()

    def test_get_expiring_documents_within_range(self):
        """get_expiring_documents returns documents expiring within N days."""
        # Add doc expiring in 15 days
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "POLICE_CHECK",
            "title": "Police Check",
            "file_reference": "s3://bucket/police.pdf",
            "file_name": "police.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "expires_at": _future_iso(days=15),
        }
        self.store.add_document(doc_dict)

        expiring = self.store.get_expiring_documents("venue_1", days_ahead=30)
        self.assertEqual(len(expiring), 1)

    def test_get_expiring_documents_outside_range(self):
        """get_expiring_documents excludes documents expiring after N days."""
        # Add doc expiring in 60 days
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "POLICE_CHECK",
            "title": "Police Check",
            "file_reference": "s3://bucket/police.pdf",
            "file_name": "police.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "expires_at": _future_iso(days=60),
        }
        self.store.add_document(doc_dict)

        expiring = self.store.get_expiring_documents("venue_1", days_ahead=30)
        self.assertEqual(len(expiring), 0)

    def test_get_expiring_documents_no_expiry(self):
        """get_expiring_documents skips documents without expiry date."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        self.store.add_document(doc_dict)

        expiring = self.store.get_expiring_documents("venue_1")
        self.assertEqual(len(expiring), 0)

    def test_get_expiring_documents_archived_excluded(self):
        """get_expiring_documents excludes archived documents."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "POLICE_CHECK",
            "title": "Police Check",
            "file_reference": "s3://bucket/police.pdf",
            "file_name": "police.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "expires_at": _future_iso(days=15),
            "status": DocumentStatus.ARCHIVED,
        }
        self.store.add_document(doc_dict)

        expiring = self.store.get_expiring_documents("venue_1")
        self.assertEqual(len(expiring), 0)

    def test_check_expirations_marks_expired(self):
        """_check_expirations marks documents as EXPIRED."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "POLICE_CHECK",
            "title": "Police Check",
            "file_reference": "s3://bucket/police.pdf",
            "file_name": "police.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _past_iso(days=60),
            "expires_at": _past_iso(days=10),
        }
        doc = self.store.add_document(doc_dict)
        self.assertEqual(doc.status, DocumentStatus.ACTIVE)

        self.store._check_expirations()

        expired_doc = self.store.get_document(doc.id)
        self.assertEqual(expired_doc.status, DocumentStatus.EXPIRED)


# ============================================================================
# Tests: Compliance Checking
# ============================================================================


class TestDocumentVaultCompliance(unittest.TestCase):
    """Tests for employee compliance checking."""

    def setUp(self):
        _reset()
        self.store = get_document_vault_store()

    def test_get_employee_compliance_all_documents_present(self):
        """get_employee_compliance returns compliant=True when all docs present."""
        # Add contract
        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        })

        # Add tax file declaration
        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "TAX_FILE_DECLARATION",
            "title": "TFN",
            "file_reference": "s3://bucket/tfn.pdf",
            "file_name": "tfn.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        })

        # Add super choice
        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "SUPER_CHOICE",
            "title": "Super Choice",
            "file_reference": "s3://bucket/super.pdf",
            "file_name": "super.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        })

        compliance = self.store.get_employee_compliance("venue_1", "emp_1")
        self.assertTrue(compliance["compliant"])
        self.assertEqual(compliance["missing_documents"], [])

    def test_get_employee_compliance_missing_documents(self):
        """get_employee_compliance identifies missing documents."""
        # Only add contract
        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        })

        compliance = self.store.get_employee_compliance("venue_1", "emp_1")
        self.assertFalse(compliance["compliant"])
        self.assertIn("TAX_FILE_DECLARATION", compliance["missing_documents"])
        self.assertIn("SUPER_CHOICE", compliance["missing_documents"])

    def test_get_employee_compliance_no_documents(self):
        """get_employee_compliance shows all missing when no documents."""
        compliance = self.store.get_employee_compliance("venue_1", "emp_1")
        self.assertFalse(compliance["compliant"])
        self.assertEqual(len(compliance["missing_documents"]), 3)

    def test_get_employee_compliance_archived_documents_excluded(self):
        """get_employee_compliance excludes archived documents."""
        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "status": DocumentStatus.ARCHIVED,
        })

        compliance = self.store.get_employee_compliance("venue_1", "emp_1")
        self.assertFalse(compliance["compliant"])
        self.assertIn("CONTRACT", compliance["missing_documents"])


# ============================================================================
# Tests: Search
# ============================================================================


class TestDocumentVaultSearch(unittest.TestCase):
    """Tests for document search functionality."""

    def setUp(self):
        _reset()
        self.store = get_document_vault_store()

        # Add test documents
        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Employment Contract 2024",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "tags": ["employment", "legal"],
        })

        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "POLICE_CHECK",
            "title": "Police Check",
            "file_reference": "s3://bucket/police.pdf",
            "file_name": "police.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "tags": ["background", "police"],
        })

    def test_search_by_title(self):
        """search_documents finds documents by title."""
        results = self.store.search_documents("venue_1", "employment")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].title, "Employment Contract 2024")

    def test_search_by_tag(self):
        """search_documents finds documents by tag."""
        results = self.store.search_documents("venue_1", "police")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].title, "Police Check")

    def test_search_case_insensitive(self):
        """search_documents is case-insensitive."""
        results = self.store.search_documents("venue_1", "EMPLOYMENT")
        self.assertEqual(len(results), 1)

    def test_search_empty_result(self):
        """search_documents returns empty list for no matches."""
        results = self.store.search_documents("venue_1", "nonexistent")
        self.assertEqual(results, [])

    def test_search_different_venue(self):
        """search_documents only searches within venue."""
        results = self.store.search_documents("venue_2", "employment")
        self.assertEqual(len(results), 0)

    def test_search_by_description(self):
        """search_documents finds documents by description."""
        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_2",
            "category": "OTHER",
            "title": "Document",
            "file_reference": "s3://bucket/doc.pdf",
            "file_name": "doc.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "description": "This is an important training certificate",
        })

        results = self.store.search_documents("venue_1", "training")
        self.assertEqual(len(results), 1)


# ============================================================================
# Tests: Edge Cases
# ============================================================================


class TestDocumentVaultEdgeCases(unittest.TestCase):
    """Tests for edge cases and error conditions."""

    def setUp(self):
        _reset()
        self.store = get_document_vault_store()

    def test_add_document_with_empty_tags(self):
        """add_document handles empty tags list."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "tags": [],
        }
        doc = self.store.add_document(doc_dict)
        self.assertEqual(doc.tags, [])

    def test_update_document_category_string_conversion(self):
        """update_document converts string category to enum."""
        doc_dict = {
            "id": "doc_1",
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        self.store.add_document(doc_dict)
        updated = self.store.update_document("doc_1", {"category": "POLICE_CHECK"})
        self.assertEqual(updated.category, DocumentCategory.POLICE_CHECK)

    def test_add_document_status_string_conversion(self):
        """add_document converts string status to enum."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "status": "ARCHIVED",
        }
        doc = self.store.add_document(doc_dict)
        self.assertEqual(doc.status, DocumentStatus.ARCHIVED)

    def test_list_documents_with_multiple_filters(self):
        """list_documents applies multiple filters correctly."""
        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        })

        self.store.add_document({
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "POLICE_CHECK",
            "title": "Police Check",
            "file_reference": "s3://bucket/police.pdf",
            "file_name": "police.pdf",
            "file_size_bytes": 51200,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        })

        # Filter by venue and category
        results = self.store.list_documents("venue_1", category="CONTRACT")
        self.assertEqual(len(results), 1)

        # Filter by venue, employee, and category
        results = self.store.list_documents(
            "venue_1", employee_id="emp_1", category="CONTRACT"
        )
        self.assertEqual(len(results), 1)

    def test_document_with_special_characters_in_fields(self):
        """Documents handle special characters in text fields."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract & Agreement (2024) - Final Version",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 102400,
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
            "tags": ["employment", "2024", "final"],
        }
        doc = self.store.add_document(doc_dict)
        self.assertEqual(doc.title, "Contract & Agreement (2024) - Final Version")

        retrieved = self.store.get_document(doc.id)
        self.assertEqual(retrieved.title, "Contract & Agreement (2024) - Final Version")

    def test_large_file_size_bytes(self):
        """Documents handle large file sizes."""
        doc_dict = {
            "venue_id": "venue_1",
            "employee_id": "emp_1",
            "category": "CONTRACT",
            "title": "Contract",
            "file_reference": "s3://bucket/contract.pdf",
            "file_name": "contract.pdf",
            "file_size_bytes": 1073741824,  # 1 GB
            "mime_type": "application/pdf",
            "uploaded_by": "hr_user",
            "uploaded_at": _now_iso(),
        }
        doc = self.store.add_document(doc_dict)
        self.assertEqual(doc.file_size_bytes, 1073741824)


if __name__ == "__main__":
    unittest.main()
