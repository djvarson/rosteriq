"""
Generate a high-fidelity PDF of the RosterIQ dashboard mockup.
Uses reportlab to replicate the dark-themed dashboard layout.
"""

from reportlab.lib.pagesizes import A3, landscape
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white, Color
from reportlab.pdfgen import canvas
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.graphics.shapes import Drawing, Rect, Line, Polygon, String, Circle, Wedge, Group
from reportlab.graphics import renderPDF
import math

# ============================================================================
# Brand colours
# ============================================================================
BG = HexColor("#0c111d")
CARD = HexColor("#161b2e")
BORDER = HexColor("#232d45")
TEXT = HexColor("#f1f5f9")
TEXT_MUTED = HexColor("#8896b3")
PRIMARY = HexColor("#3b82f6")
PRIMARY_DIM = HexColor("#1d4ed8")
ACCENT = HexColor("#10b981")
WARNING = HexColor("#f59e0b")
DANGER = HexColor("#ef4444")
PURPLE = HexColor("#a78bfa")
WHITE = HexColor("#ffffff")

# Page setup — landscape A3 for dashboard feel
PAGE_W, PAGE_H = landscape(A3)
MARGIN = 18 * mm

# ============================================================================
# Data
# ============================================================================
weekly_data = [
    {"day": "Mon", "cost": 1420, "staff": 6, "covers": 82, "forecast": 80},
    {"day": "Tue", "cost": 1580, "staff": 7, "covers": 95, "forecast": 90},
    {"day": "Wed", "cost": 1690, "staff": 7, "covers": 105, "forecast": 100},
    {"day": "Thu", "cost": 2150, "staff": 9, "covers": 125, "forecast": 120},
    {"day": "Fri", "cost": 3480, "staff": 12, "covers": 188, "forecast": 180},
    {"day": "Sat", "cost": 4120, "staff": 12, "covers": 205, "forecast": 200},
    {"day": "Sun", "cost": 2890, "staff": 10, "covers": 145, "forecast": 140},
]

hourly_staffing = [
    {"hour": "10am", "req": 2, "act": 2}, {"hour": "11am", "req": 3, "act": 3},
    {"hour": "12pm", "req": 6, "act": 6}, {"hour": "1pm", "req": 5, "act": 5},
    {"hour": "2pm", "req": 3, "act": 3}, {"hour": "3pm", "req": 2, "act": 2},
    {"hour": "4pm", "req": 3, "act": 3}, {"hour": "5pm", "req": 5, "act": 4},
    {"hour": "6pm", "req": 8, "act": 8}, {"hour": "7pm", "req": 9, "act": 9},
    {"hour": "8pm", "req": 7, "act": 7}, {"hour": "9pm", "req": 5, "act": 5},
    {"hour": "10pm", "req": 3, "act": 3},
]

employees = [
    {"name": "Sarah M.", "type": "FT", "hours": 38, "max": 38, "cost": 1496, "shifts": 5},
    {"name": "James C.", "type": "FT", "hours": 36, "max": 38, "cost": 1372, "shifts": 5},
    {"name": "Priya S.", "type": "FT", "hours": 38, "max": 38, "cost": 1496, "shifts": 5},
    {"name": "Tom W.", "type": "PT", "hours": 22, "max": 25, "cost": 812, "shifts": 4},
    {"name": "Lily N.", "type": "PT", "hours": 18, "max": 20, "cost": 642, "shifts": 3},
    {"name": "Marcus B.", "type": "PT", "hours": 24, "max": 25, "cost": 886, "shifts": 4},
    {"name": "Emma T.", "type": "CAS", "hours": 15, "max": 38, "cost": 668, "shifts": 3},
    {"name": "Jake O.", "type": "CAS", "hours": 20, "max": 38, "cost": 890, "shifts": 4},
    {"name": "Zoe P.", "type": "CAS", "hours": 12, "max": 25, "cost": 534, "shifts": 2},
    {"name": "Ryan C.", "type": "CAS", "hours": 8, "max": 20, "cost": 356, "shifts": 2},
]

cost_breakdown = [
    {"name": "Base pay", "value": 6840, "color": PRIMARY},
    {"name": "Penalty rates", "value": 1920, "color": WARNING},
    {"name": "Casual loading", "value": 612, "color": PURPLE},
    {"name": "Super (11.5%)", "value": 1078, "color": ACCENT},
]

signals = [
    {"name": "Historical", "weight": "30%", "value": "+8%", "confidence": 0.92},
    {"name": "Bookings", "weight": "25%", "value": "+12%", "confidence": 0.88},
    {"name": "POS trends", "weight": "20%", "value": "+5%", "confidence": 0.85},
    {"name": "Weather", "weight": "15%", "value": "-3%", "confidence": 0.72},
    {"name": "Events", "weight": "10%", "value": "+15%", "confidence": 0.65},
]

alerts = [
    {"type": "success", "text": "All shifts compliant with Fair Work Award MA000009", "time": "Just now"},
    {"type": "info", "text": "Projected savings vs last week: $340 (-3.2%)", "time": "1h ago"},
    {"type": "warning", "text": "Fri 7pm: Forecast 188 covers - consider calling in 1 extra", "time": "2h ago"},
    {"type": "warning", "text": "Rain forecast Sunday - demand may drop 15-20%", "time": "3h ago"},
]


# ============================================================================
# Drawing helpers
# ============================================================================

def rounded_rect(c, x, y, w, h, r, fill=None, stroke=None, stroke_width=0.5):
    """Draw a rounded rectangle."""
    c.saveState()
    if fill:
        c.setFillColor(fill)
    if stroke:
        c.setStrokeColor(stroke)
        c.setLineWidth(stroke_width)
    p = c.beginPath()
    p.roundRect(x, y, w, h, r)
    if fill and stroke:
        c.drawPath(p, fill=1, stroke=1)
    elif fill:
        c.drawPath(p, fill=1, stroke=0)
    elif stroke:
        c.drawPath(p, fill=0, stroke=1)
    c.restoreState()


def draw_text(c, x, y, text, size=10, color=TEXT, font="Helvetica", align="left"):
    c.saveState()
    c.setFillColor(color)
    c.setFont(font, size)
    if align == "right":
        w = c.stringWidth(text, font, size)
        c.drawString(x - w, y, text)
    elif align == "center":
        w = c.stringWidth(text, font, size)
        c.drawString(x - w / 2, y, text)
    else:
        c.drawString(x, y, text)
    c.restoreState()


def draw_progress_bar(c, x, y, w, h, pct, color=PRIMARY, bg=BG):
    rounded_rect(c, x, y, w, h, h / 2, fill=bg)
    if pct > 0:
        bar_w = max(h, w * min(pct, 1.0))
        rounded_rect(c, x, y, bar_w, h, h / 2, fill=color)


# ============================================================================
# KPI Cards
# ============================================================================

def draw_kpi_card(c, x, y, w, h, label, value, subtext, trend=None, trend_dir=None):
    rounded_rect(c, x, y, w, h, 3 * mm, fill=CARD, stroke=BORDER)

    # Icon placeholder
    icon_size = 9 * mm
    rounded_rect(c, x + 5 * mm, y + h - 5 * mm - icon_size, icon_size, icon_size, 2 * mm,
                 fill=HexColor("#2563eb22"))
    draw_text(c, x + 5 * mm + icon_size / 2, y + h - 5 * mm - icon_size + 2.5 * mm,
              "~", size=12, color=PRIMARY, align="center")

    # Trend
    if trend:
        trend_color = DANGER if trend_dir == "up" else ACCENT
        draw_text(c, x + w - 5 * mm, y + h - 8 * mm, trend, size=9, color=trend_color,
                  font="Helvetica-Bold", align="right")

    # Value
    draw_text(c, x + 5 * mm, y + 10 * mm, value, size=20, color=TEXT, font="Helvetica-Bold")

    # Label
    draw_text(c, x + 5 * mm, y + 4.5 * mm, label, size=9, color=TEXT_MUTED)

    # Subtext
    if subtext:
        draw_text(c, x + 5 * mm, y + 0.5 * mm, subtext, size=7.5, color=TEXT_MUTED)


# ============================================================================
# Charts
# ============================================================================

def draw_bar_chart(c, x, y, w, h, data):
    """Draw the daily cost bar chart with covers line overlay."""
    rounded_rect(c, x, y, w, h, 3 * mm, fill=CARD, stroke=BORDER)

    # Title
    draw_text(c, x + 5 * mm, y + h - 7 * mm, "Daily labour cost vs covers",
              size=11, color=TEXT, font="Helvetica-Bold")
    draw_text(c, x + 5 * mm, y + h - 12 * mm, "Click a bar to see that day's detail",
              size=8, color=TEXT_MUTED)

    # Legend
    lx = x + w - 5 * mm
    for item, col in reversed([("Cost", PRIMARY), ("Covers", ACCENT), ("Forecast", WARNING)]):
        tw = c.stringWidth(item, "Helvetica", 7.5)
        draw_text(c, lx - tw, y + h - 8 * mm, item, size=7.5, color=TEXT_MUTED)
        lx -= tw + 2 * mm
        rounded_rect(c, lx - 2.5 * mm, y + h - 8 * mm, 2.5 * mm, 2.5 * mm, 0.5 * mm, fill=col)
        lx -= 5 * mm

    # Chart area
    chart_x = x + 12 * mm
    chart_y = y + 8 * mm
    chart_w = w - 18 * mm
    chart_h = h - 28 * mm

    max_cost = max(d["cost"] for d in data) * 1.15
    max_covers = max(d["covers"] for d in data) * 1.15
    n = len(data)
    bar_gap = chart_w / n
    bar_w = bar_gap * 0.55

    # Grid lines
    c.saveState()
    c.setStrokeColor(BORDER)
    c.setLineWidth(0.3)
    c.setDash(3, 3)
    for i in range(5):
        gy = chart_y + chart_h * i / 4
        c.line(chart_x, gy, chart_x + chart_w, gy)
    c.restoreState()

    # Y-axis labels (cost)
    for i in range(5):
        val = max_cost * i / 4
        gy = chart_y + chart_h * i / 4
        draw_text(c, chart_x - 2 * mm, gy - 1.5 * mm, f"${val / 1000:.1f}k",
                  size=7, color=TEXT_MUTED, align="right")

    # Bars and lines
    cover_points = []
    forecast_points = []

    for i, d in enumerate(data):
        bx = chart_x + i * bar_gap + (bar_gap - bar_w) / 2
        bh = chart_h * d["cost"] / max_cost

        # Bar
        rounded_rect(c, bx, chart_y, bar_w, bh, 1.5 * mm, fill=PRIMARY)

        # X-axis label
        draw_text(c, chart_x + i * bar_gap + bar_gap / 2, chart_y - 4 * mm,
                  d["day"], size=8, color=TEXT_MUTED, align="center")

        # Cover point
        cx = chart_x + i * bar_gap + bar_gap / 2
        cy = chart_y + chart_h * d["covers"] / max_covers
        cover_points.append((cx, cy))

        fy = chart_y + chart_h * d["forecast"] / max_covers
        forecast_points.append((cx, fy))

    # Forecast line (dashed)
    c.saveState()
    c.setStrokeColor(WARNING)
    c.setLineWidth(1)
    c.setDash(4, 3)
    p = c.beginPath()
    for i, (px, py) in enumerate(forecast_points):
        if i == 0:
            p.moveTo(px, py)
        else:
            p.lineTo(px, py)
    c.drawPath(p, stroke=1, fill=0)
    c.restoreState()

    # Covers line (solid)
    c.saveState()
    c.setStrokeColor(ACCENT)
    c.setLineWidth(1.5)
    p = c.beginPath()
    for i, (px, py) in enumerate(cover_points):
        if i == 0:
            p.moveTo(px, py)
        else:
            p.lineTo(px, py)
    c.drawPath(p, stroke=1, fill=0)
    c.restoreState()

    # Cover dots
    for px, py in cover_points:
        c.saveState()
        c.setFillColor(ACCENT)
        c.circle(px, py, 2, fill=1, stroke=0)
        c.restoreState()


def draw_area_chart(c, x, y, w, h, data):
    """Draw the hourly staffing area chart."""
    rounded_rect(c, x, y, w, h, 3 * mm, fill=CARD, stroke=BORDER)

    draw_text(c, x + 5 * mm, y + h - 7 * mm, "Hourly staffing - Friday",
              size=11, color=TEXT, font="Helvetica-Bold")
    draw_text(c, x + 5 * mm, y + h - 12 * mm, "Required vs rostered staff by hour",
              size=8, color=TEXT_MUTED)

    # Day selector pills
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    pill_x = x + w - 5 * mm
    for d in reversed(days):
        tw = c.stringWidth(d, "Helvetica", 7.5)
        pw = tw + 4 * mm
        pill_x -= pw + 1 * mm
        if d == "Fri":
            rounded_rect(c, pill_x, y + h - 11.5 * mm, pw, 5 * mm, 1.5 * mm, fill=PRIMARY)
            draw_text(c, pill_x + pw / 2, y + h - 10 * mm, d, size=7.5, color=WHITE, align="center")
        else:
            draw_text(c, pill_x + pw / 2, y + h - 10 * mm, d, size=7.5, color=TEXT_MUTED, align="center")

    # Chart area
    chart_x = x + 10 * mm
    chart_y = y + 6 * mm
    chart_w = w - 16 * mm
    chart_h = h - 24 * mm
    max_val = max(max(d["req"] for d in data), max(d["act"] for d in data)) * 1.2
    n = len(data)

    # Grid
    c.saveState()
    c.setStrokeColor(BORDER)
    c.setLineWidth(0.3)
    c.setDash(3, 3)
    for i in range(5):
        gy = chart_y + chart_h * i / 4
        c.line(chart_x, gy, chart_x + chart_w, gy)
    c.restoreState()

    # Build points
    req_points = []
    act_points = []
    for i, d in enumerate(data):
        px = chart_x + chart_w * i / (n - 1)
        req_points.append((px, chart_y + chart_h * d["req"] / max_val))
        act_points.append((px, chart_y + chart_h * d["act"] / max_val))
        draw_text(c, px, chart_y - 4 * mm, d["hour"], size=6.5, color=TEXT_MUTED, align="center")

    # Required area fill
    c.saveState()
    c.setFillColor(HexColor("#2563eb"))
    c.setFillAlpha(0.2)
    p = c.beginPath()
    p.moveTo(req_points[0][0], chart_y)
    for px, py in req_points:
        p.lineTo(px, py)
    p.lineTo(req_points[-1][0], chart_y)
    p.close()
    c.drawPath(p, stroke=0, fill=1)
    c.restoreState()

    # Required line
    c.saveState()
    c.setStrokeColor(PRIMARY)
    c.setLineWidth(1.5)
    p = c.beginPath()
    for i, (px, py) in enumerate(req_points):
        if i == 0: p.moveTo(px, py)
        else: p.lineTo(px, py)
    c.drawPath(p, stroke=1, fill=0)
    c.restoreState()

    # Actual area fill
    c.saveState()
    c.setFillColor(ACCENT)
    c.setFillAlpha(0.12)
    p = c.beginPath()
    p.moveTo(act_points[0][0], chart_y)
    for px, py in act_points:
        p.lineTo(px, py)
    p.lineTo(act_points[-1][0], chart_y)
    p.close()
    c.drawPath(p, stroke=0, fill=1)
    c.restoreState()

    # Actual line (dashed)
    c.saveState()
    c.setStrokeColor(ACCENT)
    c.setLineWidth(1.5)
    c.setDash(3, 3)
    p = c.beginPath()
    for i, (px, py) in enumerate(act_points):
        if i == 0: p.moveTo(px, py)
        else: p.lineTo(px, py)
    c.drawPath(p, stroke=1, fill=0)
    c.restoreState()

    # Legend
    lx = x + w - 5 * mm
    draw_text(c, lx - c.stringWidth("Rostered", "Helvetica", 7.5), y + h - 16 * mm, "Rostered",
              size=7.5, color=TEXT_MUTED, align="left")
    rounded_rect(c, lx - c.stringWidth("Rostered", "Helvetica", 7.5) - 4 * mm, y + h - 16 * mm,
                 2.5 * mm, 2.5 * mm, 0.5 * mm, fill=ACCENT)
    draw_text(c, lx - c.stringWidth("Rostered", "Helvetica", 7.5) - 9 * mm - c.stringWidth("Required", "Helvetica", 7.5),
              y + h - 16 * mm, "Required", size=7.5, color=TEXT_MUTED, align="left")
    rounded_rect(c, lx - c.stringWidth("Rostered", "Helvetica", 7.5) - 13 * mm - c.stringWidth("Required", "Helvetica", 7.5),
                 y + h - 16 * mm, 2.5 * mm, 2.5 * mm, 0.5 * mm, fill=PRIMARY)


# ============================================================================
# Employee table
# ============================================================================

def draw_employee_table(c, x, y, w, h, data):
    rounded_rect(c, x, y, w, h, 3 * mm, fill=CARD, stroke=BORDER)

    draw_text(c, x + 5 * mm, y + h - 7 * mm, "Employee hours",
              size=11, color=TEXT, font="Helvetica-Bold")

    # Headers
    headers = ["Name", "Type", "Hours", "Shifts", "Cost"]
    col_x = [x + 5 * mm, x + 38 * mm, x + 55 * mm, x + 95 * mm, x + 112 * mm]
    hy = y + h - 15 * mm
    for i, hdr in enumerate(headers):
        draw_text(c, col_x[i], hy, hdr, size=7.5, color=TEXT_MUTED, font="Helvetica-Bold")

    # Separator
    c.saveState()
    c.setStrokeColor(BORDER)
    c.setLineWidth(0.5)
    c.line(x + 4 * mm, hy - 2 * mm, x + w - 4 * mm, hy - 2 * mm)
    c.restoreState()

    # Rows
    row_h = 6 * mm
    for i, emp in enumerate(data):
        ry = hy - 4 * mm - i * row_h

        draw_text(c, col_x[0], ry, emp["name"], size=8, color=TEXT, font="Helvetica-Bold")

        # Type badge
        type_colors = {"FT": PRIMARY, "PT": PURPLE, "CAS": WARNING}
        tc = type_colors.get(emp["type"], TEXT_MUTED)
        tw = c.stringWidth(emp["type"], "Helvetica-Bold", 7)
        rounded_rect(c, col_x[1], ry - 1 * mm, tw + 3 * mm, 4 * mm, 2 * mm, fill=HexColor("#00000044"))
        draw_text(c, col_x[1] + 1.5 * mm, ry, emp["type"], size=7, color=tc, font="Helvetica-Bold")

        # Hours bar
        pct = emp["hours"] / emp["max"]
        bar_color = WARNING if pct >= 1.0 else PRIMARY
        draw_progress_bar(c, col_x[2], ry + 0.5 * mm, 22 * mm, 2.5 * mm, pct, color=bar_color)
        draw_text(c, col_x[2] + 24 * mm, ry, f"{emp['hours']}/{emp['max']}h",
                  size=7, color=TEXT_MUTED)

        draw_text(c, col_x[3], ry, str(emp["shifts"]), size=8, color=TEXT_MUTED)
        draw_text(c, col_x[4], ry, f"${emp['cost']:,}", size=8, color=TEXT, font="Courier")


# ============================================================================
# Sidebar panels
# ============================================================================

def draw_cost_breakdown(c, x, y, w, h):
    rounded_rect(c, x, y, w, h, 3 * mm, fill=CARD, stroke=BORDER)
    draw_text(c, x + 5 * mm, y + h - 7 * mm, "Cost breakdown",
              size=11, color=TEXT, font="Helvetica-Bold")

    # Donut chart
    total = sum(item["value"] for item in cost_breakdown)
    center_x = x + w / 2
    center_y = y + h - 35 * mm
    outer_r = 18 * mm
    inner_r = 12 * mm

    start_angle = 90
    for item in cost_breakdown:
        sweep = 360 * item["value"] / total
        c.saveState()
        c.setFillColor(item["color"])
        # Draw wedge
        p = c.beginPath()
        p.moveTo(center_x + inner_r * math.cos(math.radians(start_angle)),
                 center_y + inner_r * math.sin(math.radians(start_angle)))
        # Outer arc
        for a in range(int(start_angle), int(start_angle + sweep) + 1):
            p.lineTo(center_x + outer_r * math.cos(math.radians(a)),
                     center_y + outer_r * math.sin(math.radians(a)))
        # Inner arc (reverse)
        for a in range(int(start_angle + sweep), int(start_angle) - 1, -1):
            p.lineTo(center_x + inner_r * math.cos(math.radians(a)),
                     center_y + inner_r * math.sin(math.radians(a)))
        p.close()
        c.drawPath(p, stroke=0, fill=1)
        c.restoreState()
        start_angle += sweep

    # Center text
    draw_text(c, center_x, center_y + 2 * mm, f"${total:,}", size=11,
              color=TEXT, font="Helvetica-Bold", align="center")
    draw_text(c, center_x, center_y - 3 * mm, "total", size=7,
              color=TEXT_MUTED, align="center")

    # Legend items
    ly = y + h - 58 * mm
    for item in cost_breakdown:
        rounded_rect(c, x + 5 * mm, ly, 2.5 * mm, 2.5 * mm, 0.5 * mm, fill=item["color"])
        draw_text(c, x + 10 * mm, ly, item["name"], size=8, color=TEXT_MUTED)
        draw_text(c, x + w - 5 * mm, ly, f"${item['value']:,}", size=8,
                  color=TEXT, font="Courier", align="right")
        ly -= 5.5 * mm

    # Total line
    ly -= 1 * mm
    c.saveState()
    c.setStrokeColor(BORDER)
    c.setLineWidth(0.5)
    c.line(x + 5 * mm, ly + 4 * mm, x + w - 5 * mm, ly + 4 * mm)
    c.restoreState()
    draw_text(c, x + 5 * mm, ly, "Total", size=9, color=TEXT, font="Helvetica-Bold")
    draw_text(c, x + w - 5 * mm, ly, f"${total:,}", size=9,
              color=TEXT, font="Courier-Bold", align="right")


def draw_signals_panel(c, x, y, w, h):
    rounded_rect(c, x, y, w, h, 3 * mm, fill=CARD, stroke=BORDER)
    draw_text(c, x + 5 * mm, y + h - 7 * mm, "Demand signals",
              size=11, color=TEXT, font="Helvetica-Bold")
    draw_text(c, x + 5 * mm, y + h - 12 * mm, "5-signal weighted variance engine",
              size=8, color=TEXT_MUTED)

    sy = y + h - 19 * mm
    for s in signals:
        # Icon placeholder
        rounded_rect(c, x + 5 * mm, sy - 1 * mm, 7 * mm, 7 * mm, 2 * mm,
                     fill=HexColor("#2563eb22"))

        draw_text(c, x + 14 * mm, sy + 3 * mm,
                  f"{s['name']} ({s['weight']})", size=8, color=TEXT)

        val_color = ACCENT if s["value"].startswith("+") else WARNING
        draw_text(c, x + w - 5 * mm, sy + 3 * mm, s["value"], size=8.5,
                  color=val_color, font="Courier-Bold", align="right")

        # Confidence bar
        conf_color = ACCENT if s["confidence"] > 0.8 else WARNING
        draw_progress_bar(c, x + 14 * mm, sy, w - 24 * mm, 2 * mm,
                          s["confidence"], color=conf_color)
        sy -= 11 * mm

    # Net variance box
    ny = sy + 3 * mm
    rounded_rect(c, x + 4 * mm, ny - 3 * mm, w - 8 * mm, 11 * mm, 2 * mm,
                 fill=HexColor("#10b98111"), stroke=HexColor("#10b98133"))
    draw_text(c, x + 8 * mm, ny + 3 * mm, "Net variance: +7.2%",
              size=9, color=ACCENT, font="Helvetica-Bold")
    draw_text(c, x + 8 * mm, ny - 1.5 * mm,
              "Demand trending above baseline", size=7, color=TEXT_MUTED)


def draw_alerts_panel(c, x, y, w, h):
    rounded_rect(c, x, y, w, h, 3 * mm, fill=CARD, stroke=BORDER)
    draw_text(c, x + 5 * mm, y + h - 7 * mm, "Alerts",
              size=11, color=TEXT, font="Helvetica-Bold")

    ay = y + h - 15 * mm
    type_colors = {"success": ACCENT, "warning": WARNING, "info": PRIMARY}
    for a in alerts:
        col = type_colors.get(a["type"], TEXT_MUTED)

        # Dot indicator
        c.saveState()
        c.setFillColor(col)
        c.circle(x + 7 * mm, ay + 2 * mm, 1.5 * mm, fill=1, stroke=0)
        c.restoreState()

        # Wrap text manually
        text = a["text"]
        max_chars = 45
        lines = []
        while text:
            if len(text) <= max_chars:
                lines.append(text)
                break
            split = text[:max_chars].rfind(" ")
            if split <= 0:
                split = max_chars
            lines.append(text[:split])
            text = text[split:].strip()

        for j, line in enumerate(lines):
            draw_text(c, x + 12 * mm, ay - j * 3.5 * mm, line, size=7.5, color=TEXT)

        draw_text(c, x + 12 * mm, ay - len(lines) * 3.5 * mm, a["time"],
                  size=6.5, color=TEXT_MUTED)
        ay -= (len(lines) + 1) * 3.5 * mm + 2 * mm


# ============================================================================
# Header
# ============================================================================

def draw_header(c):
    # Header bar
    header_h = 15 * mm
    rounded_rect(c, 0, PAGE_H - header_h, PAGE_W, header_h, 0, fill=CARD)
    c.saveState()
    c.setStrokeColor(BORDER)
    c.setLineWidth(0.5)
    c.line(0, PAGE_H - header_h, PAGE_W, PAGE_H - header_h)
    c.restoreState()

    # Logo
    logo_x = MARGIN
    logo_y = PAGE_H - header_h + 4 * mm
    rounded_rect(c, logo_x, logo_y, 8 * mm, 8 * mm, 2 * mm, fill=PRIMARY)
    draw_text(c, logo_x + 4 * mm, logo_y + 2.2 * mm, "RQ", size=9,
              color=WHITE, font="Helvetica-Bold", align="center")
    draw_text(c, logo_x + 11 * mm, logo_y + 2.2 * mm, "RosterIQ",
              size=13, color=TEXT, font="Helvetica-Bold")

    # Nav tabs
    tabs = ["overview", "roster", "forecast", "costs"]
    tab_x = logo_x + 55 * mm
    for tab in tabs:
        tw = c.stringWidth(tab, "Helvetica", 9.5)
        is_active = tab == "overview"
        draw_text(c, tab_x, logo_y + 2 * mm, tab.capitalize(),
                  size=9.5, color=PRIMARY if is_active else TEXT_MUTED,
                  font="Helvetica-Bold" if is_active else "Helvetica")
        if is_active:
            c.saveState()
            c.setStrokeColor(PRIMARY)
            c.setLineWidth(1.5)
            c.line(tab_x, PAGE_H - header_h, tab_x + tw, PAGE_H - header_h)
            c.restoreState()
        tab_x += tw + 8 * mm

    # Right side
    rx = PAGE_W - MARGIN
    # User avatar
    c.saveState()
    c.setFillColor(PRIMARY)
    c.circle(rx - 4 * mm, logo_y + 4 * mm, 4 * mm, fill=1, stroke=0)
    c.restoreState()
    draw_text(c, rx - 4 * mm, logo_y + 2.2 * mm, "DI", size=8,
              color=WHITE, font="Helvetica-Bold", align="center")

    draw_text(c, rx - 14 * mm, logo_y + 2.2 * mm, "The Royal Oak",
              size=9, color=TEXT_MUTED, align="right")

    # Week selector
    draw_text(c, rx - 60 * mm, logo_y + 2.2 * mm, "7-13 Apr 2026",
              size=9, color=TEXT_MUTED)


# ============================================================================
# Main layout
# ============================================================================

def build_dashboard(output_path):
    c = canvas.Canvas(output_path, pagesize=landscape(A3))

    # Background
    c.setFillColor(BG)
    c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    # Header
    draw_header(c)

    content_top = PAGE_H - 19 * mm
    content_bottom = MARGIN

    # KPI cards row
    kpi_h = 22 * mm
    kpi_y = content_top - kpi_h
    kpi_gap = 4 * mm
    kpi_w = (PAGE_W - 2 * MARGIN - 3 * kpi_gap) / 4

    draw_kpi_card(c, MARGIN, kpi_y, kpi_w, kpi_h,
                  "Weekly labour cost", "$17,330", "32.1% of revenue",
                  trend="-3.2%", trend_dir="down")
    draw_kpi_card(c, MARGIN + kpi_w + kpi_gap, kpi_y, kpi_w, kpi_h,
                  "Staff rostered", "10 / 12", "3 FT  ·  3 PT  ·  4 Casual")
    draw_kpi_card(c, MARGIN + 2 * (kpi_w + kpi_gap), kpi_y, kpi_w, kpi_h,
                  "Total hours", "231h", "Avg 23.1h per employee",
                  trend="+2.4%", trend_dir="up")
    draw_kpi_card(c, MARGIN + 3 * (kpi_w + kpi_gap), kpi_y, kpi_w, kpi_h,
                  "Compliance", "100%", "0 violations this week")

    # Main content area
    main_top = kpi_y - 4 * mm
    sidebar_w = 82 * mm
    gap = 4 * mm
    left_w = PAGE_W - 2 * MARGIN - sidebar_w - gap
    left_x = MARGIN
    right_x = left_x + left_w + gap

    avail_h = main_top - content_bottom

    # LEFT COLUMN
    bar_h = avail_h * 0.38
    area_h = avail_h * 0.28
    table_h = avail_h * 0.30

    draw_bar_chart(c, left_x, main_top - bar_h, left_w, bar_h, weekly_data)
    draw_area_chart(c, left_x, main_top - bar_h - gap - area_h, left_w, area_h, hourly_staffing)
    draw_employee_table(c, left_x, content_bottom, left_w, table_h, employees)

    # RIGHT COLUMN
    cost_h = avail_h * 0.36
    signal_h = avail_h * 0.36
    alert_h = avail_h * 0.24

    draw_cost_breakdown(c, right_x, main_top - cost_h, sidebar_w, cost_h)
    draw_signals_panel(c, right_x, main_top - cost_h - gap - signal_h, sidebar_w, signal_h)
    draw_alerts_panel(c, right_x, content_bottom, sidebar_w, alert_h)

    c.save()
    print(f"Dashboard PDF saved to: {output_path}")


if __name__ == "__main__":
    build_dashboard("/sessions/fervent-adoring-goodall/mnt/RosterIQ/RosterIQ_Dashboard.pdf")
