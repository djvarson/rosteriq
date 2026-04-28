"""
Generate the RosterIQ venue onboarding checklist PDF.
One-page professional handout for new pilot venues.
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether,
)
from reportlab.pdfgen import canvas

# Colours
NAVY = HexColor("#0c111d")
BLUE = HexColor("#162033")
TEAL = HexColor("#10b981")
GOLD = HexColor("#f59e0b")
LIGHT_BG = HexColor("#f8fafc")
GREY = HexColor("#64748b")
WHITE = HexColor("#ffffff")
CHECK_GREEN = HexColor("#10b981")
LIGHT_TEAL = HexColor("#ecfdf5")


def build_pdf(output_path: str = "RosterIQ_Onboarding_Checklist.pdf"):
    """Build the onboarding checklist PDF."""

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=12 * mm,
        bottomMargin=10 * mm,
    )

    styles = getSampleStyleSheet()

    # Custom styles — tighter spacing throughout to fit one page
    title_style = ParagraphStyle(
        "CustomTitle", parent=styles["Title"],
        fontSize=20, textColor=NAVY, spaceAfter=1.5 * mm,
        fontName="Helvetica-Bold", leading=24,
    )
    subtitle_style = ParagraphStyle(
        "CustomSubtitle", parent=styles["Normal"],
        fontSize=9.5, textColor=GREY, spaceAfter=4 * mm,
        fontName="Helvetica", leading=13,
    )
    section_head = ParagraphStyle(
        "SectionHead", parent=styles["Heading2"],
        fontSize=11.5, textColor=TEAL, spaceBefore=3.5 * mm,
        spaceAfter=2 * mm, fontName="Helvetica-Bold",
    )
    body_style = ParagraphStyle(
        "Body", parent=styles["Normal"],
        fontSize=9, textColor=HexColor("#2d3748"),
        fontName="Helvetica", leading=12, spaceAfter=1 * mm,
    )
    check_style = ParagraphStyle(
        "CheckItem", parent=body_style,
        leftIndent=4 * mm, spaceAfter=1.8 * mm, leading=12,
    )
    bold_body = ParagraphStyle(
        "BoldBody", parent=body_style,
        fontName="Helvetica-Bold",
    )
    footer_style = ParagraphStyle(
        "Footer", parent=styles["Normal"],
        fontSize=7.5, textColor=GREY, alignment=TA_CENTER,
        fontName="Helvetica",
    )

    story = []

    # ---- HEADER ----
    header_data = [[
        Paragraph(
            '<font color="#0f1b2d">Roster</font><font color="#00b4d8">IQ</font>',
            ParagraphStyle("Logo", fontSize=16, fontName="Helvetica-Bold")
        ),
        Paragraph(
            "Venue Onboarding Checklist",
            ParagraphStyle("HeaderRight", fontSize=9, textColor=GREY,
                           alignment=TA_RIGHT, fontName="Helvetica")
        ),
    ]]
    header_table = Table(header_data, colWidths=[80 * mm, 94 * mm])
    header_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2 * mm),
    ]))
    story.append(header_table)

    story.append(HRFlowable(
        width="100%", thickness=2, color=TEAL,
        spaceAfter=3 * mm, spaceBefore=0.5 * mm,
    ))

    story.append(Paragraph("Welcome to your RosterIQ pilot!", title_style))
    story.append(Paragraph(
        "This checklist covers everything we need from you to get started. "
        "Complete these items and we'll have your AI rostering live within a week.",
        subtitle_style,
    ))

    # ---- VENUE DETAILS BOX ----
    # Use a 2-column layout with Paragraph cells so labels don't clip
    form_style = ParagraphStyle("FormLabel", fontSize=9, textColor=NAVY, fontName="Helvetica-Bold")
    form_val = ParagraphStyle("FormVal", fontSize=9, textColor=HexColor("#2d3748"), fontName="Helvetica")

    venue_fields = [
        [Paragraph("Venue Name:", form_style),
         Paragraph("___________________________________", form_val),
         Paragraph("Contact Name:", form_style),
         Paragraph("___________________________", form_val)],
        [Paragraph("Email:", form_style),
         Paragraph("___________________________________", form_val),
         Paragraph("Phone:", form_style),
         Paragraph("___________________________", form_val)],
        [Paragraph("State:", form_style),
         Paragraph("______  Employees: ______", form_val),
         Paragraph("POS System:", form_style),
         Paragraph("___________________________", form_val)],
    ]
    venue_table = Table(venue_fields, colWidths=[28*mm, 55*mm, 28*mm, 63*mm])
    venue_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 1.5 * mm),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5 * mm),
        ("LEFTPADDING", (0, 0), (-1, -1), 2 * mm),
        ("RIGHTPADDING", (0, 0), (-1, -1), 1 * mm),
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("BOX", (0, 0), (-1, -1), 0.5, GREY),
    ]))
    story.append(venue_table)
    story.append(Spacer(1, 2 * mm))

    # ---- CHECKLIST HELPER ----
    def checkbox(text, note=None):
        """Create a checkbox item."""
        box = '<font color="#6b7b8d" size="11">\u2610</font>'
        line = f'{box}  {text}'
        if note:
            line += f'<br/><font color="#6b7b8d" size="7.5">     {note}</font>'
        return Paragraph(line, check_style)

    # ---- SECTION 1: ACCESS ----
    story.append(Paragraph("1. Access &amp; Credentials", section_head))
    story.append(checkbox(
        "<b>Tanda API access confirmed</b>",
        "Your Tanda plan must include API access (Enterprise or above). Contact Tanda support if unsure."
    ))
    story.append(checkbox(
        "<b>Tanda API credentials provided</b>",
        "Client ID and Client Secret from Tanda Settings > Integrations. We'll send a secure link to submit these."
    ))
    story.append(checkbox(
        "<b>POS system CSV export tested</b>",
        "Export 1 week of sales data from your POS (H&amp;L, Lightspeed, or Square) and send to us for validation."
    ))
    story.append(checkbox(
        "<b>Venue Wi-Fi or network access</b> (if on-premises dashboard needed)",
    ))

    # ---- SECTION 2: DATA ----
    story.append(Paragraph("2. Historical Data", section_head))
    story.append(checkbox(
        "<b>12 weeks of roster history</b> available in Tanda",
        "We pull this automatically via the API. More history = better predictions."
    ))
    story.append(checkbox(
        "<b>12 weeks of POS sales data</b> exported as CSV",
        "Daily transaction-level data with date, time, revenue, and covers/guests if available."
    ))
    story.append(checkbox(
        "<b>Upcoming events or closures</b> flagged",
        "Tell us about any one-off events, renovations, or holidays in the next 4 weeks."
    ))

    # ---- SECTION 3: CONFIG ----
    story.append(Paragraph("3. Venue Configuration", section_head))
    story.append(checkbox(
        "<b>Employee list reviewed</b> in Tanda",
        "Ensure employment types (FT/PT/Casual), hourly rates, and award levels are current."
    ))
    story.append(checkbox(
        "<b>Minimum staffing levels</b> provided per role",
        "e.g., Bar: min 2, Kitchen: min 3, Floor: min 2. We use this as a safety floor."
    ))
    story.append(checkbox(
        "<b>Target labour percentage</b> agreed",
        "What's your target labour cost as a % of revenue? Industry average is 28-35%."
    ))
    story.append(checkbox(
        "<b>Trading hours confirmed</b>",
        "Standard open/close times for each day of the week."
    ))

    # ---- SECTION 4: WHAT HAPPENS NEXT ----
    story.append(Paragraph("4. What Happens Next", section_head))

    tl_body = ParagraphStyle("TLBody", parent=body_style, fontSize=8.5, leading=11, spaceAfter=0)
    tl_bold = ParagraphStyle("TLBold", parent=tl_body, fontName="Helvetica-Bold")

    timeline_data = [
        [Paragraph("<b>Day 1-2</b>", tl_bold),
         Paragraph("We connect to your Tanda account and import historical data.", tl_body)],
        [Paragraph("<b>Day 3-5</b>", tl_bold),
         Paragraph("AI model trains on your venue's patterns. POS data imported and validated.", tl_body)],
        [Paragraph("<b>Day 6-7</b>", tl_bold),
         Paragraph("First AI-generated roster delivered alongside your existing roster for comparison.", tl_body)],
        [Paragraph("<b>Week 2-4</b>", tl_bold),
         Paragraph("Model accuracy improves daily. Weekly cost reports show savings vs your old rosters.", tl_body)],
        [Paragraph("<b>Week 4</b>", tl_bold),
         Paragraph("Pilot review meeting. We present results and agree on next steps together.", tl_body)],
    ]
    timeline_table = Table(timeline_data, colWidths=[20 * mm, 154 * mm])
    timeline_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 1 * mm),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1 * mm),
        ("LINEBELOW", (0, 0), (-1, -2), 0.3, HexColor("#e2e8f0")),
    ]))
    story.append(timeline_table)

    # ---- CONTACT BOX ----
    story.append(Spacer(1, 3 * mm))
    story.append(HRFlowable(width="100%", thickness=1, color=TEAL, spaceAfter=2 * mm))

    contact_data = [[
        Paragraph(
            '<b>Questions?</b> Contact your RosterIQ onboarding team:',
            ParagraphStyle("ContactLabel", fontSize=8.5, textColor=NAVY, fontName="Helvetica"),
        ),
        Paragraph(
            'hello@rosteriq.com.au',
            ParagraphStyle("ContactInfo", fontSize=8.5, textColor=TEAL,
                           fontName="Helvetica-Bold", alignment=TA_RIGHT),
        ),
    ]]
    contact_table = Table(contact_data, colWidths=[104 * mm, 70 * mm])
    contact_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_TEAL),
        ("BOX", (0, 0), (-1, -1), 0.5, TEAL),
        ("TOPPADDING", (0, 0), (-1, -1), 2.5 * mm),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5 * mm),
        ("LEFTPADDING", (0, 0), (-1, -1), 3 * mm),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3 * mm),
    ]))
    story.append(contact_table)

    # ---- FOOTER ----
    story.append(Spacer(1, 2 * mm))
    story.append(Paragraph(
        "RosterIQ  |  AI-Powered Rostering for Australian Hospitality  |  rosteriq.com.au",
        footer_style,
    ))

    # Build
    doc.build(story)
    print(f"PDF generated: {output_path}")


if __name__ == "__main__":
    build_pdf()
