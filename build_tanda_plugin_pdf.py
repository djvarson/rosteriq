"""
Generate a multi-page PDF showing RosterIQ as a Tanda plugin.
Page 1: Tanda desktop dashboard with RosterIQ widget integrated
Page 2: Tanda mobile app with RosterIQ AI roster tab
Page 3: Integration flow diagram — how the plugin connects
"""

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white, black, Color
from reportlab.pdfgen import canvas
import math

# ============================================================================
# Brand palettes
# ============================================================================

# Tanda — based on their product: clean white, dark indigo sidebar, purple accent
TANDA_BG = HexColor("#f5f7fa")
TANDA_WHITE = HexColor("#ffffff")
TANDA_SIDEBAR = HexColor("#1a1a2e")
TANDA_SIDEBAR_ACTIVE = HexColor("#2d2d50")
TANDA_PURPLE = HexColor("#6c63ff")
TANDA_PURPLE_LIGHT = HexColor("#ebe9ff")
TANDA_TEXT = HexColor("#2d3748")
TANDA_TEXT_MUTED = HexColor("#8492a6")
TANDA_BORDER = HexColor("#e2e8f0")
TANDA_GREEN = HexColor("#48bb78")
TANDA_RED = HexColor("#fc5c65")
TANDA_ORANGE = HexColor("#f7b731")

# RosterIQ — our accent blue/teal
RIQ_PRIMARY = HexColor("#3b82f6")
RIQ_ACCENT = HexColor("#10b981")
RIQ_DARK = HexColor("#0c111d")
RIQ_CARD = HexColor("#161b2e")
RIQ_GRADIENT_START = HexColor("#3b82f6")
RIQ_GRADIENT_END = HexColor("#10b981")

PAGE_W, PAGE_H = landscape(A4)

# ============================================================================
# Drawing helpers
# ============================================================================

def rounded_rect(c, x, y, w, h, r, fill=None, stroke=None, sw=0.5):
    c.saveState()
    if fill: c.setFillColor(fill)
    if stroke: c.setStrokeColor(stroke); c.setLineWidth(sw)
    p = c.beginPath(); p.roundRect(x, y, w, h, r)
    c.drawPath(p, fill=1 if fill else 0, stroke=1 if stroke else 0)
    c.restoreState()

def text(c, x, y, t, size=10, color=TANDA_TEXT, font="Helvetica", align="left"):
    c.saveState(); c.setFillColor(color); c.setFont(font, size)
    if align == "right": x -= c.stringWidth(t, font, size)
    elif align == "center": x -= c.stringWidth(t, font, size) / 2
    c.drawString(x, y, t); c.restoreState()

def draw_bar(c, x, y, w, h, pct, color=RIQ_PRIMARY, bg=HexColor("#e2e8f0")):
    rounded_rect(c, x, y, w, h, h/2, fill=bg)
    if pct > 0: rounded_rect(c, x, y, max(h, w*min(pct,1)), h, h/2, fill=color)

def gradient_rect(c, x, y, w, h, c1, c2, steps=30):
    """Simple horizontal gradient using thin strips."""
    sw = w / steps
    for i in range(steps):
        frac = i / max(steps - 1, 1)
        r = c1.red + (c2.red - c1.red) * frac
        g = c1.green + (c2.green - c1.green) * frac
        b = c1.blue + (c2.blue - c1.blue) * frac
        c.saveState()
        c.setFillColor(Color(r, g, b))
        c.rect(x + i * sw, y, sw + 0.5, h, fill=1, stroke=0)
        c.restoreState()


# ============================================================================
# Page 1: Tanda Desktop Dashboard with RosterIQ Plugin
# ============================================================================

def draw_page1_desktop(c):
    """Tanda desktop dashboard with RosterIQ widget integrated."""
    # Background
    c.setFillColor(TANDA_BG)
    c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    # Title banner
    gradient_rect(c, 0, PAGE_H - 18*mm, PAGE_W, 18*mm, RIQ_DARK, HexColor("#1a1a2e"))
    text(c, 15*mm, PAGE_H - 12*mm, "RosterIQ for Tanda  —  Desktop Dashboard View",
         size=14, color=white, font="Helvetica-Bold")
    text(c, PAGE_W - 15*mm, PAGE_H - 12*mm, "AI-Powered Rostering Plugin",
         size=10, color=HexColor("#94a3b8"), align="right")

    dash_top = PAGE_H - 22*mm
    dash_left = 0

    # --- Tanda sidebar ---
    sb_w = 42*mm
    rounded_rect(c, dash_left, 0, sb_w, dash_top, 0, fill=TANDA_SIDEBAR)

    # Tanda logo
    text(c, dash_left + 10*mm, dash_top - 10*mm, "tanda", size=14,
         color=white, font="Helvetica-Bold")

    # Sidebar nav items
    nav_items = [
        ("Dashboard", True), ("Roster", False), ("Timesheets", False),
        ("Leave", False), ("Reports", False), ("RosterIQ", "plugin"),
    ]
    ny = dash_top - 24*mm
    for label, active in nav_items:
        if active == "plugin":
            # RosterIQ plugin item — highlighted differently
            gradient_rect(c, dash_left + 2*mm, ny - 1*mm, sb_w - 4*mm, 8*mm,
                          RIQ_PRIMARY, RIQ_ACCENT, steps=20)
            text(c, dash_left + 7*mm, ny + 1*mm, label, size=9,
                 color=white, font="Helvetica-Bold")
            # Small "AI" badge
            rounded_rect(c, dash_left + sb_w - 14*mm, ny + 0.5*mm, 9*mm, 5*mm,
                         2*mm, fill=HexColor("#ffffff33"))
            text(c, dash_left + sb_w - 9.5*mm, ny + 1.2*mm, "AI", size=6.5,
                 color=white, font="Helvetica-Bold", align="center")
        elif active:
            rounded_rect(c, dash_left + 2*mm, ny - 1*mm, sb_w - 4*mm, 8*mm,
                         2*mm, fill=TANDA_SIDEBAR_ACTIVE)
            text(c, dash_left + 7*mm, ny + 1*mm, label, size=9,
                 color=white, font="Helvetica-Bold")
        else:
            text(c, dash_left + 7*mm, ny + 1*mm, label, size=9,
                 color=HexColor("#8892b0"))
        ny -= 11*mm

    # --- Main content area ---
    mx = sb_w + 4*mm
    mw = PAGE_W - sb_w - 8*mm

    # Top bar
    rounded_rect(c, mx, dash_top - 12*mm, mw, 10*mm, 2*mm, fill=TANDA_WHITE, stroke=TANDA_BORDER)
    text(c, mx + 4*mm, dash_top - 8*mm, "Dashboard", size=12,
         color=TANDA_TEXT, font="Helvetica-Bold")
    text(c, mx + mw - 4*mm, dash_top - 8*mm, "The Royal Oak  |  Mon 7 Apr - Sun 13 Apr 2026",
         size=8, color=TANDA_TEXT_MUTED, align="right")

    content_top = dash_top - 16*mm

    # --- RosterIQ Plugin Widget (hero position, top) ---
    riq_h = 52*mm
    rounded_rect(c, mx, content_top - riq_h, mw, riq_h, 3*mm, fill=RIQ_DARK, stroke=RIQ_PRIMARY, sw=1.5)

    # Plugin header
    rounded_rect(c, mx + 4*mm, content_top - 10*mm, 8*mm, 6*mm, 2*mm, fill=RIQ_PRIMARY)
    text(c, mx + 8*mm, content_top - 8.5*mm, "RQ", size=7,
         color=white, font="Helvetica-Bold", align="center")
    text(c, mx + 15*mm, content_top - 8*mm, "RosterIQ  —  AI Roster Optimiser",
         size=10, color=white, font="Helvetica-Bold")
    text(c, mx + mw - 4*mm, content_top - 7*mm, "PLUGIN ACTIVE",
         size=7, color=RIQ_ACCENT, font="Helvetica-Bold", align="right")

    # KPI row inside plugin widget
    kpi_y = content_top - 20*mm
    kpi_w = (mw - 20*mm) / 4
    kpis = [
        ("Weekly Cost", "$17,330", "-3.2%", RIQ_ACCENT),
        ("Staff Rostered", "10 / 12", "", None),
        ("Compliance", "100%", "0 violations", RIQ_ACCENT),
        ("Forecast Accuracy", "87%", "+2.1%", RIQ_ACCENT),
    ]
    for i, (label, val, sub, sub_color) in enumerate(kpis):
        kx = mx + 5*mm + i * kpi_w
        rounded_rect(c, kx, kpi_y - 12*mm, kpi_w - 3*mm, 14*mm, 2*mm,
                     fill=RIQ_CARD)
        text(c, kx + 3*mm, kpi_y - 2*mm, val, size=14, color=white, font="Helvetica-Bold")
        text(c, kx + 3*mm, kpi_y - 7*mm, label, size=7, color=HexColor("#94a3b8"))
        if sub:
            text(c, kx + kpi_w - 6*mm, kpi_y - 2*mm, sub, size=7,
                 color=sub_color or HexColor("#94a3b8"), align="right")

    # Mini bar chart inside plugin widget
    chart_x = mx + 5*mm
    chart_y = content_top - riq_h + 4*mm
    chart_w = mw * 0.45
    chart_h = 14*mm
    text(c, chart_x, chart_y + chart_h + 2*mm, "Daily Labour Cost", size=7,
         color=HexColor("#94a3b8"))
    costs = [1420, 1580, 1690, 2150, 3480, 4120, 2890]
    days = ["M", "T", "W", "T", "F", "S", "S"]
    max_c = max(costs)
    bar_gap = chart_w / 7
    for i, cost in enumerate(costs):
        bx = chart_x + i * bar_gap
        bh = chart_h * cost / max_c
        rounded_rect(c, bx + 1*mm, chart_y, bar_gap - 2*mm, bh, 1*mm, fill=RIQ_PRIMARY)
        text(c, bx + bar_gap/2, chart_y - 3*mm, days[i], size=5.5,
             color=HexColor("#64748b"), align="center")

    # Demand signals mini
    sig_x = mx + mw * 0.52
    sig_y = chart_y
    text(c, sig_x, chart_y + chart_h + 2*mm, "Demand Signals", size=7,
         color=HexColor("#94a3b8"))
    sigs = [("Historical", 0.92, "+8%"), ("Bookings", 0.88, "+12%"),
            ("POS", 0.85, "+5%"), ("Weather", 0.72, "-3%"), ("Events", 0.65, "+15%")]
    for i, (name, conf, val) in enumerate(sigs):
        sy = sig_y + chart_h - i * 3.2*mm
        text(c, sig_x, sy, name, size=5.5, color=HexColor("#94a3b8"))
        draw_bar(c, sig_x + 18*mm, sy + 0.3*mm, 25*mm, 1.8*mm, conf,
                 color=RIQ_ACCENT if conf > 0.8 else TANDA_ORANGE,
                 bg=HexColor("#334155"))
        val_col = RIQ_ACCENT if val.startswith("+") else TANDA_ORANGE
        text(c, sig_x + 46*mm, sy, val, size=5.5, color=val_col, font="Courier-Bold")

    # Net variance
    rounded_rect(c, sig_x + 52*mm, sig_y + 2*mm, 22*mm, 12*mm, 2*mm,
                 fill=HexColor("#10b98122"), stroke=HexColor("#10b98155"))
    text(c, sig_x + 63*mm, sig_y + 9*mm, "+7.2%", size=11,
         color=RIQ_ACCENT, font="Helvetica-Bold", align="center")
    text(c, sig_x + 63*mm, sig_y + 4*mm, "net variance", size=5.5,
         color=HexColor("#94a3b8"), align="center")

    # --- Tanda native widgets below ---
    tw_top = content_top - riq_h - 4*mm
    tw_left_w = mw * 0.48
    tw_right_w = mw * 0.48
    tw_gap = mw * 0.04
    tw_h = tw_top - 4*mm

    # Left: Live Attendance (Tanda native widget)
    rounded_rect(c, mx, 4*mm, tw_left_w, tw_h, 3*mm, fill=TANDA_WHITE, stroke=TANDA_BORDER)
    text(c, mx + 4*mm, tw_h - 2*mm, "Live Attendance", size=10,
         color=TANDA_TEXT, font="Helvetica-Bold")
    # Mini attendance dots
    staff_status = [
        ("Sarah M.", "On shift", TANDA_GREEN),
        ("James C.", "On shift", TANDA_GREEN),
        ("Priya S.", "On shift", TANDA_GREEN),
        ("Tom W.", "Starting 2pm", TANDA_ORANGE),
        ("Lily N.", "Off today", TANDA_TEXT_MUTED),
        ("Marcus B.", "On shift", TANDA_GREEN),
        ("Emma T.", "Starting 5pm", TANDA_ORANGE),
        ("Jake O.", "Off today", TANDA_TEXT_MUTED),
    ]
    ay = tw_h - 10*mm
    for name, status, color_dot in staff_status:
        if ay < 8*mm: break
        c.saveState()
        c.setFillColor(color_dot)
        c.circle(mx + 7*mm, ay + 1.5*mm, 1.5*mm, fill=1, stroke=0)
        c.restoreState()
        text(c, mx + 11*mm, ay, name, size=8, color=TANDA_TEXT, font="Helvetica-Bold")
        text(c, mx + tw_left_w - 4*mm, ay, status, size=7,
             color=TANDA_TEXT_MUTED, align="right")
        ay -= 6.5*mm

    # Right: Wage Tracker (Tanda native with RosterIQ enhancement)
    rx = mx + tw_left_w + tw_gap
    rounded_rect(c, rx, 4*mm, tw_right_w, tw_h, 3*mm, fill=TANDA_WHITE, stroke=TANDA_BORDER)
    text(c, rx + 4*mm, tw_h - 2*mm, "Wage Tracker", size=10,
         color=TANDA_TEXT, font="Helvetica-Bold")

    # "Enhanced by RosterIQ" badge
    rounded_rect(c, rx + tw_right_w - 34*mm, tw_h - 3.5*mm, 30*mm, 5*mm, 2*mm, fill=RIQ_PRIMARY)
    text(c, rx + tw_right_w - 19*mm, tw_h - 2.5*mm, "Enhanced by RosterIQ",
         size=6, color=white, font="Helvetica-Bold", align="center")

    # Wage data
    wage_items = [
        ("Roster Cost (today)", "$2,480", TANDA_TEXT),
        ("Actual Timesheets", "$2,310", TANDA_TEXT),
        ("Variance", "-$170", TANDA_GREEN),
        ("Labour % of Revenue", "31.2%", TANDA_TEXT),
        ("Target", "32.0%", TANDA_TEXT_MUTED),
    ]
    wy = tw_h - 12*mm
    for label, val, col in wage_items:
        text(c, rx + 4*mm, wy, label, size=8, color=TANDA_TEXT_MUTED)
        text(c, rx + tw_right_w - 4*mm, wy, val, size=9, color=col,
             font="Courier-Bold", align="right")
        wy -= 6.5*mm

    # RosterIQ insight box
    wy -= 2*mm
    rounded_rect(c, rx + 3*mm, wy - 12*mm, tw_right_w - 6*mm, 14*mm, 2*mm,
                 fill=HexColor("#2563eb11"), stroke=HexColor("#2563eb44"))
    text(c, rx + 6*mm, wy + -2*mm, "RosterIQ Insight:", size=7.5,
         color=RIQ_PRIMARY, font="Helvetica-Bold")
    text(c, rx + 6*mm, wy - 6*mm, "You're $170 under budget today. Friday forecast",
         size=7, color=TANDA_TEXT)
    text(c, rx + 6*mm, wy - 10*mm, "shows +12% demand — consider 1 extra for 6-9pm.",
         size=7, color=TANDA_TEXT)


# ============================================================================
# Page 2: Mobile App View
# ============================================================================

def draw_page2_mobile(c):
    """Tanda mobile app with RosterIQ AI roster tab."""
    c.setFillColor(TANDA_BG)
    c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    # Title banner
    gradient_rect(c, 0, PAGE_H - 18*mm, PAGE_W, 18*mm, RIQ_DARK, HexColor("#1a1a2e"))
    text(c, 15*mm, PAGE_H - 12*mm, "RosterIQ for Tanda  —  Mobile App Integration",
         size=14, color=white, font="Helvetica-Bold")
    text(c, PAGE_W - 15*mm, PAGE_H - 12*mm, "Staff & Manager Views",
         size=10, color=HexColor("#94a3b8"), align="right")

    content_top = PAGE_H - 24*mm

    # Draw 3 phone mockups side by side
    phone_w = 58*mm
    phone_h = 120*mm
    phone_r = 5*mm
    gap = 18*mm
    total_w = 3 * phone_w + 2 * gap
    start_x = (PAGE_W - total_w) / 2
    phone_y = (content_top - phone_h) / 2 - 2*mm

    phones = [
        ("Manager: AI Roster", "manager_roster"),
        ("Staff: My Shifts", "staff_shifts"),
        ("Live: Demand Alert", "demand_alert"),
    ]

    for idx, (title, ptype) in enumerate(phones):
        px = start_x + idx * (phone_w + gap)
        py = phone_y

        # Phone frame
        rounded_rect(c, px, py, phone_w, phone_h, phone_r, fill=HexColor("#1a1a2e"),
                     stroke=HexColor("#334155"), sw=1)

        # Status bar
        sbar_y = py + phone_h - 6*mm
        text(c, px + 4*mm, sbar_y + 1*mm, "9:41", size=6, color=white, font="Helvetica-Bold")
        text(c, px + phone_w - 4*mm, sbar_y + 1*mm, "100%", size=5, color=white, align="right")

        # Tanda header
        hdr_y = sbar_y - 8*mm
        rounded_rect(c, px, hdr_y, phone_w, 8*mm, 0, fill=TANDA_PURPLE)
        text(c, px + 4*mm, hdr_y + 2.5*mm, "tanda", size=8,
             color=white, font="Helvetica-Bold")
        # RosterIQ badge
        rounded_rect(c, px + phone_w - 20*mm, hdr_y + 1.5*mm, 17*mm, 5*mm, 2*mm,
                     fill=HexColor("#ffffff33"))
        text(c, px + phone_w - 11.5*mm, hdr_y + 2.5*mm, "RosterIQ",
             size=5.5, color=white, font="Helvetica-Bold", align="center")

        # Content area
        ct = hdr_y - 1*mm
        content_bg = TANDA_WHITE
        rounded_rect(c, px + 1*mm, py + 12*mm, phone_w - 2*mm, ct - py - 12*mm, 0, fill=content_bg)

        # Phone label
        text(c, px + phone_w/2, py + 5*mm, title, size=8,
             color=HexColor("#94a3b8"), font="Helvetica-Bold", align="center")

        if ptype == "manager_roster":
            _draw_manager_roster(c, px + 3*mm, py + 14*mm, phone_w - 6*mm, ct - py - 16*mm)
        elif ptype == "staff_shifts":
            _draw_staff_shifts(c, px + 3*mm, py + 14*mm, phone_w - 6*mm, ct - py - 16*mm)
        elif ptype == "demand_alert":
            _draw_demand_alert(c, px + 3*mm, py + 14*mm, phone_w - 6*mm, ct - py - 16*mm)

        # Bottom nav bar
        nav_y = py + 12*mm
        rounded_rect(c, px + 1*mm, py + 1*mm, phone_w - 2*mm, 11*mm, 0, fill=TANDA_WHITE)
        nav_tabs = ["Home", "Roster", "Clock", "RosterIQ"]
        ntw = (phone_w - 2*mm) / len(nav_tabs)
        for ni, nt in enumerate(nav_tabs):
            nx = px + 1*mm + ni * ntw + ntw/2
            is_active = (ptype == "manager_roster" and nt == "RosterIQ") or \
                        (ptype == "staff_shifts" and nt == "Roster") or \
                        (ptype == "demand_alert" and nt == "RosterIQ")
            col = RIQ_PRIMARY if (nt == "RosterIQ" and is_active) else \
                  TANDA_PURPLE if is_active else TANDA_TEXT_MUTED
            text(c, nx, py + 4*mm, nt, size=5.5, color=col,
                 font="Helvetica-Bold" if is_active else "Helvetica", align="center")
            if is_active:
                c.saveState()
                c.setFillColor(col)
                c.circle(nx, py + 8.5*mm, 1*mm, fill=1, stroke=0)
                c.restoreState()


def _draw_manager_roster(c, x, y, w, h):
    """Manager roster view inside phone."""
    # Week header
    text(c, x + 2*mm, y + h - 4*mm, "AI-Optimised Roster", size=8,
         color=TANDA_TEXT, font="Helvetica-Bold")
    text(c, x + w - 2*mm, y + h - 4*mm, "Week of 7 Apr", size=6,
         color=TANDA_TEXT_MUTED, align="right")

    # Cost summary
    cy = y + h - 12*mm
    rounded_rect(c, x, cy, w, 10*mm, 2*mm, fill=RIQ_DARK)
    text(c, x + 3*mm, cy + 5*mm, "$17,330", size=10, color=white, font="Helvetica-Bold")
    text(c, x + 3*mm, cy + 1*mm, "Weekly cost  |  -3.2%", size=5.5, color=RIQ_ACCENT)
    text(c, x + w - 3*mm, cy + 5*mm, "100%", size=8, color=RIQ_ACCENT,
         font="Helvetica-Bold", align="right")
    text(c, x + w - 3*mm, cy + 1*mm, "Compliant", size=5.5,
         color=HexColor("#94a3b8"), align="right")

    # Day rows
    days_data = [
        ("Mon 7", 6, "$1,420"), ("Tue 8", 7, "$1,580"),
        ("Wed 9", 7, "$1,690"), ("Thu 10", 9, "$2,150"),
        ("Fri 11", 12, "$3,480"), ("Sat 12", 12, "$4,120"),
        ("Sun 13", 10, "$2,890"),
    ]
    dy = cy - 4*mm
    for day, staff, cost in days_data:
        if dy < y + 2*mm: break
        rounded_rect(c, x, dy - 5.5*mm, w, 5.5*mm, 1.5*mm,
                     fill=TANDA_BG, stroke=TANDA_BORDER, sw=0.3)
        text(c, x + 2*mm, dy - 4*mm, day, size=6, color=TANDA_TEXT, font="Helvetica-Bold")
        text(c, x + 20*mm, dy - 4*mm, f"{staff} staff", size=5.5, color=TANDA_TEXT_MUTED)
        text(c, x + w - 2*mm, dy - 4*mm, cost, size=6, color=TANDA_TEXT,
             font="Courier-Bold", align="right")
        dy -= 7*mm


def _draw_staff_shifts(c, x, y, w, h):
    """Staff shift view inside phone."""
    text(c, x + 2*mm, y + h - 4*mm, "My Shifts", size=8,
         color=TANDA_TEXT, font="Helvetica-Bold")
    text(c, x + w - 2*mm, y + h - 4*mm, "Sarah M.", size=6,
         color=TANDA_TEXT_MUTED, align="right")

    shifts = [
        ("Mon 7 Apr", "9:00am - 5:00pm", "Bar", "Confirmed", TANDA_GREEN),
        ("Tue 8 Apr", "10:00am - 6:00pm", "Floor", "Confirmed", TANDA_GREEN),
        ("Wed 9 Apr", "OFF", "", "", TANDA_TEXT_MUTED),
        ("Thu 10 Apr", "2:00pm - 10:00pm", "Bar", "Pending", TANDA_ORANGE),
        ("Fri 11 Apr", "5:00pm - 11:00pm", "Bar + Close", "Pending", TANDA_ORANGE),
        ("Sat 12 Apr", "10:00am - 6:00pm", "Floor", "Pending", TANDA_ORANGE),
        ("Sun 13 Apr", "OFF", "", "", TANDA_TEXT_MUTED),
    ]
    sy = y + h - 12*mm
    for day, times, role, status, col in shifts:
        if sy < y + 2*mm: break
        card_h = 10*mm if times != "OFF" else 6*mm
        rounded_rect(c, x, sy - card_h, w, card_h, 1.5*mm,
                     fill=TANDA_WHITE, stroke=TANDA_BORDER, sw=0.3)
        # Color strip on left
        if times != "OFF":
            rounded_rect(c, x, sy - card_h, 1.5*mm, card_h, 0.5*mm, fill=col)

        text(c, x + 3*mm, sy - 4*mm, day, size=6, color=TANDA_TEXT, font="Helvetica-Bold")
        if times == "OFF":
            text(c, x + 28*mm, sy - 4*mm, "Day Off", size=6, color=TANDA_TEXT_MUTED)
        else:
            text(c, x + 3*mm, sy - 8*mm, times, size=5.5, color=TANDA_TEXT_MUTED)
            text(c, x + w - 3*mm, sy - 4*mm, role, size=5.5, color=TANDA_PURPLE,
                 font="Helvetica-Bold", align="right")
            text(c, x + w - 3*mm, sy - 8*mm, status, size=5, color=col, align="right")
        sy -= card_h + 2*mm


def _draw_demand_alert(c, x, y, w, h):
    """Live demand alert view inside phone."""
    text(c, x + 2*mm, y + h - 4*mm, "Live Demand", size=8,
         color=TANDA_TEXT, font="Helvetica-Bold")
    text(c, x + w - 2*mm, y + h - 4*mm, "Right now", size=6,
         color=RIQ_ACCENT, align="right")

    # Alert card
    ay = y + h - 12*mm
    rounded_rect(c, x, ay - 16*mm, w, 16*mm, 2*mm,
                 fill=HexColor("#f59e0b11"), stroke=HexColor("#f59e0b55"))
    text(c, x + 3*mm, ay - 3*mm, "Demand Alert", size=7,
         color=TANDA_ORANGE, font="Helvetica-Bold")
    text(c, x + 3*mm, ay - 7*mm, "Fri 7pm forecast: 188 covers", size=6, color=TANDA_TEXT)
    text(c, x + 3*mm, ay - 11*mm, "Currently rostered: 8 staff", size=6, color=TANDA_TEXT)
    text(c, x + 3*mm, ay - 15*mm, "Recommend: Call in 1 extra", size=6,
         color=TANDA_ORANGE, font="Helvetica-Bold")

    # Suggestion card
    sy = ay - 22*mm
    rounded_rect(c, x, sy - 18*mm, w, 18*mm, 2*mm,
                 fill=HexColor("#2563eb11"), stroke=HexColor("#2563eb44"))
    text(c, x + 3*mm, sy - 3*mm, "Suggested Call-In", size=7,
         color=RIQ_PRIMARY, font="Helvetica-Bold")
    text(c, x + 3*mm, sy - 8*mm, "Emma T. (CAS)", size=6.5, color=TANDA_TEXT, font="Helvetica-Bold")
    text(c, x + 3*mm, sy - 12*mm, "Available  |  5pm-11pm  |  $35.63/h", size=5.5,
         color=TANDA_TEXT_MUTED)
    # Action buttons
    rounded_rect(c, x + 2*mm, sy - 17*mm, 20*mm, 5*mm, 2*mm, fill=RIQ_PRIMARY)
    text(c, x + 12*mm, sy - 15.8*mm, "Call In", size=6,
         color=white, font="Helvetica-Bold", align="center")
    rounded_rect(c, x + 25*mm, sy - 17*mm, 20*mm, 5*mm, 2*mm,
                 fill=TANDA_WHITE, stroke=TANDA_BORDER)
    text(c, x + 35*mm, sy - 15.8*mm, "Skip", size=6,
         color=TANDA_TEXT_MUTED, align="center")

    # Weather signal
    wy = sy - 24*mm
    rounded_rect(c, x, wy - 10*mm, w, 10*mm, 2*mm, fill=TANDA_BG)
    text(c, x + 3*mm, wy - 3*mm, "Weather Signal", size=6.5,
         color=TANDA_TEXT, font="Helvetica-Bold")
    text(c, x + 3*mm, wy - 7*mm, "Rain forecast Sun — demand may drop 15-20%",
         size=5.5, color=TANDA_TEXT_MUTED)

    # Compliance badge
    by = wy - 15*mm
    rounded_rect(c, x, by - 8*mm, w, 8*mm, 2*mm, fill=HexColor("#10b98111"), stroke=HexColor("#10b98133"))
    text(c, x + 3*mm, by - 5*mm, "All shifts compliant  —  Fair Work MA000009",
         size=6, color=RIQ_ACCENT, font="Helvetica-Bold")


# ============================================================================
# Page 3: Integration Architecture
# ============================================================================

def draw_page3_integration(c):
    """Integration flow diagram."""
    c.setFillColor(TANDA_WHITE)
    c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    # Title banner
    gradient_rect(c, 0, PAGE_H - 18*mm, PAGE_W, 18*mm, RIQ_DARK, HexColor("#1a1a2e"))
    text(c, 15*mm, PAGE_H - 12*mm, "RosterIQ for Tanda  —  Integration Architecture",
         size=14, color=white, font="Helvetica-Bold")
    text(c, PAGE_W - 15*mm, PAGE_H - 12*mm, "How the Plugin Connects",
         size=10, color=HexColor("#94a3b8"), align="right")

    ct = PAGE_H - 28*mm
    cx = PAGE_W / 2

    # --- Top section: Data Sources ---
    text(c, cx, ct, "DATA SOURCES", size=9, color=TANDA_TEXT_MUTED,
         font="Helvetica-Bold", align="center")
    ct -= 5*mm

    sources = [
        ("Tanda API", "Rosters, timesheets,\nemployees, leave", TANDA_PURPLE),
        ("POS System", "Sales, covers,\ntransaction data", TANDA_ORANGE),
        ("Weather API", "Forecasts, rain,\ntemperature", HexColor("#38bdf8")),
        ("Events Feed", "Local events,\npublic holidays", HexColor("#a78bfa")),
        ("Bookings", "Reservations,\nfunction bookings", RIQ_ACCENT),
    ]
    box_w = 38*mm
    box_h = 18*mm
    total = len(sources) * box_w + (len(sources) - 1) * 5*mm
    sx = (PAGE_W - total) / 2

    for i, (name, desc, col) in enumerate(sources):
        bx = sx + i * (box_w + 5*mm)
        by = ct - box_h
        rounded_rect(c, bx, by, box_w, box_h, 2*mm, fill=TANDA_WHITE,
                     stroke=col, sw=1.5)
        rounded_rect(c, bx, by + box_h - 4*mm, box_w, 4*mm, 2*mm, fill=col)
        text(c, bx + box_w/2, by + box_h - 3*mm, name, size=7,
             color=white, font="Helvetica-Bold", align="center")
        lines = desc.split("\n")
        for j, line in enumerate(lines):
            text(c, bx + box_w/2, by + box_h - 9*mm - j*3.5*mm, line,
                 size=6, color=TANDA_TEXT_MUTED, align="center")

        # Arrow down
        arrow_x = bx + box_w/2
        c.saveState()
        c.setStrokeColor(col)
        c.setLineWidth(1)
        c.line(arrow_x, by, arrow_x, by - 8*mm)
        # arrowhead
        c.setFillColor(col)
        p = c.beginPath()
        p.moveTo(arrow_x, by - 10*mm)
        p.lineTo(arrow_x - 2*mm, by - 7*mm)
        p.lineTo(arrow_x + 2*mm, by - 7*mm)
        p.close()
        c.drawPath(p, fill=1, stroke=0)
        c.restoreState()

    # --- Middle: RosterIQ Engine ---
    engine_y = ct - box_h - 14*mm
    engine_h = 38*mm
    engine_w = PAGE_W * 0.65
    engine_x = (PAGE_W - engine_w) / 2

    # Gradient background for engine
    gradient_rect(c, engine_x, engine_y - engine_h, engine_w, engine_h,
                  RIQ_DARK, HexColor("#162033"))
    rounded_rect(c, engine_x, engine_y - engine_h, engine_w, engine_h, 3*mm,
                 stroke=RIQ_PRIMARY, sw=2)

    # Engine header
    text(c, engine_x + engine_w/2, engine_y - 6*mm,
         "RosterIQ AI Engine", size=14, color=white, font="Helvetica-Bold", align="center")

    # Engine modules
    modules = [
        ("Demand\nForecaster", "XGBoost +\nProphet", RIQ_PRIMARY),
        ("Variance\nEngine", "5-signal\nweighted", RIQ_ACCENT),
        ("Award\nCompliance", "Fair Work\nMA000009", TANDA_ORANGE),
        ("Roster\nOptimiser", "Constraint\nsolver", TANDA_PURPLE),
        ("Decision\nEngine", "Cut / call-in\nranking", HexColor("#f472b6")),
    ]
    mod_w = 28*mm
    mod_h = 18*mm
    mtotal = len(modules) * mod_w + (len(modules) - 1) * 4*mm
    mod_sx = engine_x + (engine_w - mtotal) / 2
    mod_y = engine_y - engine_h + 5*mm

    for i, (name, desc, col) in enumerate(modules):
        mx = mod_sx + i * (mod_w + 4*mm)
        rounded_rect(c, mx, mod_y, mod_w, mod_h, 2*mm, fill=RIQ_CARD, stroke=col, sw=0.8)
        lines = name.split("\n")
        for j, line in enumerate(lines):
            text(c, mx + mod_w/2, mod_y + mod_h - 5*mm - j*3.5*mm, line,
                 size=6.5, color=white, font="Helvetica-Bold", align="center")
        desc_lines = desc.split("\n")
        for j, line in enumerate(desc_lines):
            text(c, mx + mod_w/2, mod_y + 5*mm - j*3*mm, line,
                 size=5, color=HexColor("#94a3b8"), align="center")

        # Arrows between modules
        if i < len(modules) - 1:
            ax = mx + mod_w + 0.5*mm
            ay = mod_y + mod_h/2
            c.saveState()
            c.setStrokeColor(HexColor("#475569"))
            c.setLineWidth(0.8)
            c.line(ax, ay, ax + 3*mm, ay)
            c.setFillColor(HexColor("#475569"))
            p = c.beginPath()
            p.moveTo(ax + 3.5*mm, ay)
            p.lineTo(ax + 2*mm, ay + 1*mm)
            p.lineTo(ax + 2*mm, ay - 1*mm)
            p.close()
            c.drawPath(p, fill=1, stroke=0)
            c.restoreState()

    # --- Bottom: Outputs ---
    out_y = engine_y - engine_h - 14*mm

    # Arrows down from engine
    for off in [-0.25, 0, 0.25]:
        ax = engine_x + engine_w/2 + off * engine_w
        c.saveState()
        c.setStrokeColor(RIQ_PRIMARY)
        c.setLineWidth(1)
        c.line(ax, engine_y - engine_h, ax, out_y + 20*mm)
        c.setFillColor(RIQ_PRIMARY)
        p = c.beginPath()
        p.moveTo(ax, out_y + 18*mm)
        p.lineTo(ax - 2*mm, out_y + 21*mm)
        p.lineTo(ax + 2*mm, out_y + 21*mm)
        p.close()
        c.drawPath(p, fill=1, stroke=0)
        c.restoreState()

    text(c, cx, out_y + 14*mm, "OUTPUTS", size=9, color=TANDA_TEXT_MUTED,
         font="Helvetica-Bold", align="center")

    outputs = [
        ("Optimised\nRosters", "Pushed back\nto Tanda", RIQ_PRIMARY),
        ("Cost\nReports", "Weekly savings\nbreakdown", RIQ_ACCENT),
        ("Live\nAlerts", "Overstaffed /\nunderstaffed", TANDA_ORANGE),
        ("Dashboard\nWidget", "Inside Tanda\nUI", TANDA_PURPLE),
        ("Mobile\nNotifications", "Push alerts\nto managers", HexColor("#f472b6")),
    ]
    obox_w = 34*mm
    obox_h = 18*mm
    ototal = len(outputs) * obox_w + (len(outputs) - 1) * 6*mm
    osx = (PAGE_W - ototal) / 2

    for i, (name, desc, col) in enumerate(outputs):
        ox = osx + i * (obox_w + 6*mm)
        oy = out_y - obox_h + 10*mm
        rounded_rect(c, ox, oy, obox_w, obox_h, 2*mm, fill=TANDA_WHITE,
                     stroke=col, sw=1.5)
        lines = name.split("\n")
        for j, line in enumerate(lines):
            text(c, ox + obox_w/2, oy + obox_h - 5*mm - j*3.5*mm, line,
                 size=6.5, color=TANDA_TEXT, font="Helvetica-Bold", align="center")
        desc_lines = desc.split("\n")
        for j, line in enumerate(desc_lines):
            text(c, ox + obox_w/2, oy + 5*mm - j*3*mm, line,
                 size=5, color=TANDA_TEXT_MUTED, align="center")

    # Footer note
    text(c, cx, 8*mm,
         "RosterIQ connects via Tanda's REST API (OAuth2)  |  No changes to your existing Tanda setup  |  $3/employee/month",
         size=8, color=TANDA_TEXT_MUTED, align="center")


# ============================================================================
# Build
# ============================================================================

def main():
    output = "/sessions/fervent-adoring-goodall/mnt/RosterIQ/RosterIQ_Tanda_Plugin.pdf"
    c = canvas.Canvas(output, pagesize=landscape(A4))

    draw_page1_desktop(c)
    c.showPage()

    draw_page2_mobile(c)
    c.showPage()

    draw_page3_integration(c)
    c.save()

    print(f"PDF saved: {output}")


if __name__ == "__main__":
    main()
