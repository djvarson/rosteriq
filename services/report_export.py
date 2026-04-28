"""
Report export service for RosterIQ compliance reports.

Exports compliance reports to PDF and CSV formats for easy sharing
and analysis in spreadsheet or document review tools.
"""

from io import BytesIO, StringIO
import csv
from datetime import datetime

from rosteriq.services.compliance_reports import (
    ComplianceReport,
    ComplianceSection,
    ViolationSeverity,
)


# ============================================================================
# PDF EXPORT (using reportlab)
# ============================================================================


def export_compliance_pdf(report: ComplianceReport) -> bytes:
    """
    Generate a PDF export of a compliance report.

    Uses reportlab to create a formatted PDF with:
    - Header with venue and date range
    - Executive summary with compliance score
    - Detailed sections with tables
    - Colour-coded violation severity

    Args:
        report: ComplianceReport to export

    Returns:
        PDF document as bytes
    """
    try:
        from reportlab.lib.pagesizes import letter, A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            SimpleDocTemplate,
            Table,
            TableStyle,
            Paragraph,
            Spacer,
            PageBreak,
            Image,
        )
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    except ImportError:
        raise ImportError(
            "reportlab is required for PDF export. "
            "Install with: pip install reportlab"
        )

    # Create PDF document
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )

    # Container for the 'Flowable' objects
    elements = []

    # Styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "CustomTitle",
        parent=styles["Heading1"],
        fontSize=24,
        textColor=colors.HexColor("#1f4788"),
        spaceAfter=12,
        alignment=TA_CENTER,
    )
    heading_style = ParagraphStyle(
        "CustomHeading",
        parent=styles["Heading2"],
        fontSize=14,
        textColor=colors.HexColor("#1f4788"),
        spaceAfter=10,
        spaceBefore=10,
    )

    # Title
    title = Paragraph("RosterIQ Fair Work Compliance Report", title_style)
    elements.append(title)
    elements.append(Spacer(1, 0.2 * inch))

    # Header information
    header_data = [
        ["Venue ID:", report.venue_id, "Report Date:", report.generated_at.strftime("%d %b %Y")],
        ["Period:", f"{report.period_start} to {report.period_end}", "", ""],
    ]
    header_table = Table(header_data, colWidths=[1.2 * inch, 2 * inch, 1.2 * inch, 2 * inch])
    header_table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#1f4788")),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.grey),
                ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#f0f0f0")]),
            ]
        )
    )
    elements.append(header_table)
    elements.append(Spacer(1, 0.3 * inch))

    # Compliance score card
    score_color = {
        "green": colors.HexColor("#2ecc71"),
        "amber": colors.HexColor("#f39c12"),
        "red": colors.HexColor("#e74c3c"),
    }.get(report.score_rating, colors.grey)

    score_data = [
        [
            "Overall Compliance Score",
            f"{report.overall_score:.1f}%",
            report.score_rating.upper(),
        ]
    ]
    score_table = Table(score_data, colWidths=[3 * inch, 1.5 * inch, 1.5 * inch])
    score_table.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 12),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f9f9f9")),
                ("TEXTCOLOR", (2, 0), (2, 0), score_color),
                ("ALIGN", (1, 0), (-1, 0), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LINEBELOW", (0, 0), (-1, -1), 1, colors.black),
            ]
        )
    )
    elements.append(score_table)
    elements.append(Spacer(1, 0.3 * inch))

    # Section summaries
    for section in report.sections:
        section_heading = Paragraph(section.title, heading_style)
        elements.append(section_heading)

        # Section data
        section_data = [
            ["Compliance Rate:", f"{section.compliance_percentage:.1f}%", "Violations:", str(len(section.violations))],
            ["Description:", section.description, "", ""],
        ]
        section_table = Table(section_data, colWidths=[1.2 * inch, 2 * inch, 1.2 * inch, 2 * inch])
        section_table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.lightgrey),
                    ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#fafafa")]),
                ]
            )
        )
        elements.append(section_table)
        elements.append(Spacer(1, 0.1 * inch))

        # Findings
        if section.findings:
            finding_items = "\n".join([f"• {f}" for f in section.findings[:5]])  # First 5
            if len(section.findings) > 5:
                finding_items += f"\n• ... and {len(section.findings) - 5} more findings"
            findings_para = Paragraph(f"<b>Findings:</b><br/>{finding_items}", styles["Normal"])
            elements.append(findings_para)
            elements.append(Spacer(1, 0.2 * inch))

    # Violations detail
    if report.violations:
        elements.append(PageBreak())
        violations_heading = Paragraph("Detailed Violations", heading_style)
        elements.append(violations_heading)

        violations_data = [["Employee", "Type", "Description", "Severity"]]
        for v in report.violations[:50]:  # First 50
            violations_data.append(
                [
                    v.employee_name,
                    v.violation_type,
                    v.description[:50],  # Truncate long descriptions
                    v.severity.value.upper(),
                ]
            )

        if len(report.violations) > 50:
            violations_data.append(
                ["", "", f"... and {len(report.violations) - 50} more", ""]
            )

        violations_table = Table(
            violations_data,
            colWidths=[1.5 * inch, 1.2 * inch, 2.5 * inch, 1 * inch],
        )

        # Color code severity
        table_style = [
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4788")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]

        # Add row colors based on severity
        for i, row in enumerate(violations_data[1:], 1):
            if "CRITICAL" in str(row[-1]):
                table_style.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#ffe6e6")))
            elif "WARNING" in str(row[-1]):
                table_style.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#fff3cd")))

        violations_table.setStyle(TableStyle(table_style))
        elements.append(violations_table)

    # Footer
    elements.append(Spacer(1, 0.3 * inch))
    footer_text = (
        f"Generated by RosterIQ on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        "Fair Work Compliance Engine | MA000009 Hospitality Award"
    )
    footer_para = Paragraph(
        f"<i>{footer_text}</i>",
        ParagraphStyle(
            "Footer",
            parent=styles["Normal"],
            fontSize=8,
            textColor=colors.grey,
            alignment=TA_CENTER,
        ),
    )
    elements.append(footer_para)

    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()


# ============================================================================
# CSV EXPORT
# ============================================================================


def export_compliance_csv(report: ComplianceReport) -> str:
    """
    Generate a CSV export of a compliance report.

    Suitable for import into spreadsheet applications for further analysis.
    Includes violations, findings, and section summaries.

    Args:
        report: ComplianceReport to export

    Returns:
        CSV content as string
    """
    output = StringIO()
    writer = csv.writer(output)

    # Header section
    writer.writerow(["RosterIQ Fair Work Compliance Report"])
    writer.writerow([])
    writer.writerow(["Report Information"])
    writer.writerow(["Venue ID", report.venue_id])
    writer.writerow(["Period Start", report.period_start.isoformat()])
    writer.writerow(["Period End", report.period_end.isoformat()])
    writer.writerow(["Generated", report.generated_at.isoformat()])
    writer.writerow(["Overall Score", f"{report.overall_score:.1f}%"])
    writer.writerow(["Rating", report.score_rating.upper()])
    writer.writerow([])

    # Section summaries
    writer.writerow(["Compliance Section Summary"])
    writer.writerow(["Section", "Compliance %", "Violations", "Findings"])

    for section in report.sections:
        writer.writerow(
            [
                section.title,
                f"{section.compliance_percentage:.1f}%",
                len(section.violations),
                len(section.findings),
            ]
        )

    writer.writerow([])

    # Detailed violations
    writer.writerow(["Violations Detail"])
    writer.writerow(
        [
            "Employee ID",
            "Employee Name",
            "Violation Type",
            "Description",
            "Severity",
            "Shift ID",
            "Date",
        ]
    )

    for violation in report.violations:
        writer.writerow(
            [
                violation.employee_id,
                violation.employee_name,
                violation.violation_type,
                violation.description,
                violation.severity.value,
                violation.shift_id or "",
                violation.date.isoformat() if violation.date else "",
            ]
        )

    writer.writerow([])

    # Detailed findings
    writer.writerow(["All Findings"])
    writer.writerow(["Section", "Finding"])

    for section in report.sections:
        for finding in section.findings:
            writer.writerow([section.title, finding])

    return output.getvalue()
