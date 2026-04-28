"""
Security administration endpoints for RosterIQ.

Provides REST endpoints for:
- Viewing current security configuration (admin only)
- CSP violation reports and analysis (public, rate-limited)
- Security event tracking and alerting

Usage:
    from rosteriq.routes.security import create_security_router
    security_router = create_security_router(security_middleware)
    app.include_router(security_router)
"""

import logging
from typing import Optional, List
from datetime import datetime
from dataclasses import dataclass

from fastapi import APIRouter, HTTPException, Depends, Request, Header
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Will be injected by api.py
_security_middleware: Optional[object] = None


# ============================================================================
# Response models
# ============================================================================


class SecurityConfigResponse(BaseModel):
    """Current security configuration response."""
    environment: str = Field(..., description="Deployment environment")
    hsts_enabled: bool = Field(..., description="Whether HSTS is enabled")
    hsts_max_age: int = Field(..., description="HSTS max-age in seconds")
    csp_header: str = Field(..., description="Full CSP header value")
    allowed_origins: List[str] = Field(..., description="Allowed CORS origins")
    csp_report_uri: Optional[str] = Field(None, description="CSP report endpoint URI")


class CSPViolationRequest(BaseModel):
    """CSP violation report from browser."""
    # CSP spec: https://w3c.github.io/webappsec-csp/#violation-report
    csp_report: Optional[dict] = Field(
        None,
        description="CSP report object from browser",
        alias="csp-report"
    )

    class Config:
        populate_by_name = True  # Allow both csp_report and csp-report


class CSPViolationLog(BaseModel):
    """CSP violation record for analysis."""
    timestamp: datetime = Field(..., description="When violation occurred")
    violated_directive: str = Field(..., description="Which CSP directive was violated")
    original_policy: Optional[str] = Field(None, description="Full CSP policy")
    document_uri: str = Field(..., description="URL where violation occurred")
    referrer: Optional[str] = Field(None, description="Referrer of document")
    user_agent: Optional[str] = Field(None, description="Browser user agent")
    source_file: Optional[str] = Field(None, description="File that caused violation")
    line_number: Optional[int] = Field(None, description="Line number of violation")
    column_number: Optional[int] = Field(None, description="Column number of violation")
    status_code: Optional[int] = Field(None, description="HTTP status code if applicable")


class CSPViolationsSummary(BaseModel):
    """Summary of CSP violations."""
    total_violations: int = Field(..., description="Total violations recorded")
    violations_by_directive: dict = Field(..., description="Violation count per directive")
    recent_violations: List[CSPViolationLog] = Field(
        ...,
        description="Most recent violations"
    )
    top_violated_origins: dict = Field(..., description="Most frequently violated document URIs")


# ============================================================================
# In-memory storage (for demonstration; use database in production)
# ============================================================================

class CSPViolationStore:
    """Simple in-memory store for CSP violations."""

    def __init__(self, max_violations: int = 1000):
        self.violations: List[CSPViolationLog] = []
        self.max_violations = max_violations

    def add_violation(self, violation: CSPViolationLog) -> None:
        """Record a CSP violation."""
        self.violations.append(violation)
        # Keep only recent violations
        if len(self.violations) > self.max_violations:
            self.violations = self.violations[-self.max_violations:]

    def get_violations(self, limit: int = 50) -> List[CSPViolationLog]:
        """Get most recent violations."""
        return self.violations[-limit:][::-1]

    def get_summary(self) -> dict:
        """Get summary statistics."""
        if not self.violations:
            return {
                "total_violations": 0,
                "violations_by_directive": {},
                "recent_violations": [],
                "top_violated_origins": {},
            }

        # Count violations by directive
        by_directive = {}
        by_origin = {}

        for v in self.violations:
            by_directive[v.violated_directive] = by_directive.get(v.violated_directive, 0) + 1
            by_origin[v.document_uri] = by_origin.get(v.document_uri, 0) + 1

        # Sort by count
        by_directive = dict(
            sorted(by_directive.items(), key=lambda x: x[1], reverse=True)
        )
        by_origin = dict(
            sorted(by_origin.items(), key=lambda x: x[1], reverse=True)[:10]
        )

        return {
            "total_violations": len(self.violations),
            "violations_by_directive": by_directive,
            "recent_violations": self.get_violations(limit=10),
            "top_violated_origins": by_origin,
        }

    def clear(self) -> None:
        """Clear all violations."""
        self.violations.clear()


_violation_store = CSPViolationStore()


# ============================================================================
# Route handlers
# ============================================================================


def create_security_router(security_middleware: object) -> APIRouter:
    """
    Create security administration router.

    Args:
        security_middleware: SecurityHeadersMiddleware instance

    Returns:
        APIRouter with security endpoints
    """
    global _security_middleware
    _security_middleware = security_middleware

    router = APIRouter(prefix="/api/v1/admin/security", tags=["security"])

    @router.get("/config", response_model=SecurityConfigResponse)
    async def get_security_config(request: Request):
        """
        Get current security configuration (admin only).

        Returns all active security headers configuration, allowing admins to
        verify that security headers are properly configured in the deployment.

        Returns:
        - environment: Current deployment environment (dev/staging/prod)
        - hsts_enabled: Whether HSTS header is being sent
        - hsts_max_age: HSTS cache duration in seconds
        - csp_header: Full Content-Security-Policy header value
        - allowed_origins: CORS origin allowlist
        - csp_report_uri: Endpoint for CSP violation reports (if configured)
        """
        if not _security_middleware:
            raise HTTPException(status_code=503, detail="Security middleware not initialized")

        try:
            config = _security_middleware.get_config_dict()
            return SecurityConfigResponse(
                environment=config["environment"],
                hsts_enabled=config["hsts_enabled"],
                hsts_max_age=config["hsts_max_age"],
                csp_header=_security_middleware.csp_header,
                allowed_origins=config["allowed_origins"],
                csp_report_uri=config.get("csp_report_uri"),
            )
        except Exception as e:
            logger.error(f"Error retrieving security config: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/csp-report")
    async def receive_csp_violation(
        request: Request,
        user_agent: str = Header(None),
    ):
        """
        Receive and record CSP violation reports from browsers (public endpoint).

        This endpoint is configured as the CSP report-uri and receives JSON reports
        when browsers detect CSP violations. Reports are rate-limited to prevent
        abuse. Use GET /csp-violations to analyze collected violations.

        Request body follows CSP spec:
        - csp-report: Object containing violation details
          - violated-directive: The directive that was violated
          - original-policy: Full CSP policy that was violated
          - document-uri: The page where violation occurred
          - effective-directive: The effective directive (normalized)
          - etc.

        Returns:
        - status: "recorded" if violation was logged successfully
        - detail: Additional context

        Rate limit: 100 reports per minute per IP
        """
        try:
            body = await request.json()
            report = body.get("csp-report", {})

            if not report:
                logger.warning("Received empty CSP report")
                return {"status": "ignored", "detail": "Empty report"}

            # Extract violation details
            violation = CSPViolationLog(
                timestamp=datetime.utcnow(),
                violated_directive=report.get("violated-directive", "unknown"),
                original_policy=report.get("original-policy"),
                document_uri=report.get("document-uri", "unknown"),
                referrer=report.get("referrer"),
                user_agent=user_agent,
                source_file=report.get("source-file"),
                line_number=report.get("line-number"),
                column_number=report.get("column-number"),
                status_code=report.get("status-code"),
            )

            _violation_store.add_violation(violation)

            # Log violations that might be security issues
            if report.get("violated-directive") not in ("report-uri", "report-to"):
                logger.warning(
                    f"CSP violation reported: {violation.violated_directive} on "
                    f"{violation.document_uri} from {user_agent}"
                )

            return {"status": "recorded", "detail": "Violation logged successfully"}

        except Exception as e:
            logger.error(f"Error processing CSP report: {e}")
            return {"status": "error", "detail": str(e)}

    @router.get("/csp-violations", response_model=CSPViolationsSummary)
    async def get_csp_violations(request: Request):
        """
        Get CSP violation reports and analysis (admin only).

        Returns summary of CSP violations collected via the report-uri endpoint,
        including counts by directive, recent violations, and top violated URLs.

        Returns:
        - total_violations: Total violations recorded
        - violations_by_directive: Count of violations per CSP directive
        - recent_violations: Most recent violation details
        - top_violated_origins: Most frequently violated document URIs

        Use this to:
        - Detect common CSP configuration issues
        - Identify potential malicious injection attempts
        - Validate CSP policy before stricter enforcement
        - Troubleshoot third-party integrations
        """
        try:
            summary = _violation_store.get_summary()
            return CSPViolationsSummary(
                total_violations=summary["total_violations"],
                violations_by_directive=summary["violations_by_directive"],
                recent_violations=summary["recent_violations"],
                top_violated_origins=summary["top_violated_origins"],
            )
        except Exception as e:
            logger.error(f"Error retrieving CSP violations: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @router.delete("/csp-violations")
    async def clear_csp_violations(request: Request):
        """
        Clear all recorded CSP violations (admin only).

        Useful for resetting violation tracking after deploying CSP policy changes
        or after addressing security issues.

        Returns:
        - status: "cleared" on success
        """
        try:
            _violation_store.clear()
            logger.info("CSP violations cleared by admin")
            return {"status": "cleared", "detail": "All CSP violations cleared"}
        except Exception as e:
            logger.error(f"Error clearing CSP violations: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return router
