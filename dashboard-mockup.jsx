import { useState } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell, Area, AreaChart } from "recharts";
import { Users, DollarSign, Clock, AlertTriangle, TrendingDown, CheckCircle, ChevronDown, Calendar, Sun, CloudRain, Zap, ArrowUpRight, ArrowDownRight } from "lucide-react";

const BRAND = { primary: "#3b82f6", accent: "#10b981", warning: "#f59e0b", danger: "#ef4444", purple: "#a78bfa", muted: "#64748b", bg: "#0c111d", card: "#161b2e", cardHover: "#1e2640", border: "#232d45", text: "#f1f5f9", textMuted: "#8896b3" };

const weeklyData = [
  { day: "Mon", cost: 1420, staff: 6, covers: 82, forecast: 80 },
  { day: "Tue", cost: 1580, staff: 7, covers: 95, forecast: 90 },
  { day: "Wed", cost: 1690, staff: 7, covers: 105, forecast: 100 },
  { day: "Thu", cost: 2150, staff: 9, covers: 125, forecast: 120 },
  { day: "Fri", cost: 3480, staff: 12, covers: 188, forecast: 180 },
  { day: "Sat", cost: 4120, staff: 12, covers: 205, forecast: 200 },
  { day: "Sun", cost: 2890, staff: 10, covers: 145, forecast: 140 },
];

const hourlyStaffing = [
  { hour: "10am", required: 2, actual: 2 }, { hour: "11am", required: 3, actual: 3 },
  { hour: "12pm", required: 6, actual: 6 }, { hour: "1pm", required: 5, actual: 5 },
  { hour: "2pm", required: 3, actual: 3 }, { hour: "3pm", required: 2, actual: 2 },
  { hour: "4pm", required: 3, actual: 3 }, { hour: "5pm", required: 5, actual: 4 },
  { hour: "6pm", required: 8, actual: 8 }, { hour: "7pm", required: 9, actual: 9 },
  { hour: "8pm", required: 7, actual: 7 }, { hour: "9pm", required: 5, actual: 5 },
  { hour: "10pm", required: 3, actual: 3 },
];

const employeeData = [
  { name: "Sarah M.", type: "FT", hours: 38, max: 38, cost: 1496, shifts: 5, status: "on-track" },
  { name: "James C.", type: "FT", hours: 36, max: 38, cost: 1372, shifts: 5, status: "on-track" },
  { name: "Priya S.", type: "FT", hours: 38, max: 38, cost: 1496, shifts: 5, status: "on-track" },
  { name: "Tom W.", type: "PT", hours: 22, max: 25, cost: 812, shifts: 4, status: "on-track" },
  { name: "Lily N.", type: "PT", hours: 18, max: 20, cost: 642, shifts: 3, status: "on-track" },
  { name: "Marcus B.", type: "PT", hours: 24, max: 25, cost: 886, shifts: 4, status: "on-track" },
  { name: "Emma T.", type: "CAS", hours: 15, max: 38, cost: 668, shifts: 3, status: "on-track" },
  { name: "Jake O.", type: "CAS", hours: 20, max: 38, cost: 890, shifts: 4, status: "on-track" },
  { name: "Zoe P.", type: "CAS", hours: 12, max: 25, cost: 534, shifts: 2, status: "warning" },
  { name: "Ryan C.", type: "CAS", hours: 8, max: 20, cost: 356, shifts: 2, status: "on-track" },
];

const costBreakdown = [
  { name: "Base pay", value: 6840, color: BRAND.primary },
  { name: "Penalty rates", value: 1920, color: BRAND.warning },
  { name: "Casual loading", value: 612, color: "#a78bfa" },
  { name: "Super (11.5%)", value: 1078, color: BRAND.accent },
];

const alerts = [
  { type: "success", icon: CheckCircle, text: "All shifts compliant with Fair Work Award MA000009", time: "Just now" },
  { type: "info", icon: TrendingDown, text: "Projected savings vs last week: $340 (−3.2%)", time: "1h ago" },
  { type: "warning", icon: AlertTriangle, text: "Fri 7pm: Forecast 188 covers — consider calling in 1 extra", time: "2h ago" },
  { type: "warning", icon: CloudRain, text: "Rain forecast Sunday — demand may drop 15–20%", time: "3h ago" },
];

const signals = [
  { name: "Historical", weight: "30%", value: "+8%", confidence: 0.92, icon: Clock },
  { name: "Bookings", weight: "25%", value: "+12%", confidence: 0.88, icon: Calendar },
  { name: "POS trends", weight: "20%", value: "+5%", confidence: 0.85, icon: DollarSign },
  { name: "Weather", weight: "15%", value: "−3%", confidence: 0.72, icon: Sun },
  { name: "Events", weight: "10%", value: "+15%", confidence: 0.65, icon: Zap },
];

function StatCard({ icon: Icon, label, value, subtext, trend, trendDir }) {
  return (
    <div style={{ background: BRAND.card, borderRadius: 12, padding: "20px 24px", border: `1px solid ${BRAND.border}` }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
        <div style={{ background: `${BRAND.primary}22`, borderRadius: 8, padding: 8 }}>
          <Icon size={20} color={BRAND.primary} />
        </div>
        {trend && (
          <div style={{ display: "flex", alignItems: "center", gap: 2, fontSize: 13, fontWeight: 600, color: trendDir === "up" ? BRAND.danger : BRAND.accent }}>
            {trendDir === "up" ? <ArrowUpRight size={14} /> : <ArrowDownRight size={14} />}
            {trend}
          </div>
        )}
      </div>
      <div style={{ fontSize: 28, fontWeight: 700, color: BRAND.text, lineHeight: 1.1 }}>{value}</div>
      <div style={{ fontSize: 13, color: BRAND.textMuted, marginTop: 4 }}>{label}</div>
      {subtext && <div style={{ fontSize: 12, color: BRAND.textMuted, marginTop: 2 }}>{subtext}</div>}
    </div>
  );
}

function TypeBadge({ type }) {
  const colors = { FT: BRAND.primary, PT: BRAND.purple, CAS: BRAND.warning };
  return (
    <span style={{ fontSize: 10, fontWeight: 700, padding: "3px 8px", borderRadius: 6, background: `${colors[type]}1e`, color: colors[type], letterSpacing: "0.3px", textTransform: "uppercase" }}>{type}</span>
  );
}

export default function Dashboard() {
  const [activeDay, setActiveDay] = useState("Fri");
  const [activeTab, setActiveTab] = useState("overview");

  const tabs = ["overview", "roster", "forecast", "costs"];

  return (
    <div style={{ minHeight: "100vh", background: BRAND.bg, color: BRAND.text, fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif", WebkitFontSmoothing: "antialiased" }}>
      {/* Header */}
      <div style={{ background: BRAND.card, borderBottom: `1px solid ${BRAND.border}`, padding: "0 32px" }}>
        <div style={{ maxWidth: 1400, margin: "0 auto", display: "flex", alignItems: "center", justifyContent: "space-between", height: 64 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 24 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <div style={{ width: 32, height: 32, borderRadius: 8, background: `linear-gradient(135deg, ${BRAND.primary}, ${BRAND.accent})`, display: "flex", alignItems: "center", justifyContent: "center", fontWeight: 800, fontSize: 14, color: "#fff" }}>RQ</div>
              <span style={{ fontWeight: 700, fontSize: 18, color: BRAND.text }}>RosterIQ</span>
            </div>
            <div style={{ display: "flex", gap: 0 }}>
              {tabs.map(tab => (
                <button key={tab} onClick={() => setActiveTab(tab)} style={{ padding: "20px 16px", fontSize: 14, fontWeight: activeTab === tab ? 600 : 400, color: activeTab === tab ? BRAND.primary : BRAND.textMuted, background: "none", border: "none", borderBottom: activeTab === tab ? `2px solid ${BRAND.primary}` : "2px solid transparent", cursor: "pointer", textTransform: "capitalize" }}>{tab}</button>
              ))}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 14px", borderRadius: 8, background: BRAND.bg, border: `1px solid ${BRAND.border}`, fontSize: 14, color: BRAND.textMuted }}>
              <Calendar size={14} />
              <span>7–13 Apr 2026</span>
              <ChevronDown size={14} />
            </div>
            <div style={{ fontSize: 14, color: BRAND.textMuted }}>The Royal Oak</div>
            <div style={{ width: 32, height: 32, borderRadius: 9999, background: BRAND.primary, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 13, fontWeight: 600 }}>DI</div>
          </div>
        </div>
      </div>

      {/* Main content */}
      <div style={{ maxWidth: 1400, margin: "0 auto", padding: "24px 32px" }}>
        {/* KPI Cards */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginBottom: 24 }}>
          <StatCard icon={DollarSign} label="Weekly labour cost" value="$17,330" subtext="32.1% of revenue" trend="−3.2%" trendDir="down" />
          <StatCard icon={Users} label="Staff rostered" value="10 / 12" subtext="3 FT · 3 PT · 4 Casual" />
          <StatCard icon={Clock} label="Total hours" value="231h" subtext="Avg 23.1h per employee" trend="+2.4%" trendDir="up" />
          <StatCard icon={CheckCircle} label="Compliance" value="100%" subtext="0 violations this week" />
        </div>

        {/* Main grid: charts + sidebar */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 380px", gap: 16 }}>
          {/* Left column */}
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            {/* Daily cost + covers chart */}
            <div style={{ background: BRAND.card, borderRadius: 12, padding: 24, border: `1px solid ${BRAND.border}` }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
                <div>
                  <h3 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>Daily labour cost vs covers</h3>
                  <p style={{ margin: "4px 0 0", fontSize: 13, color: BRAND.textMuted }}>Click a bar to see that day's detail</p>
                </div>
                <div style={{ display: "flex", gap: 16, fontSize: 12, color: BRAND.textMuted }}>
                  <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ width: 10, height: 10, borderRadius: 2, background: BRAND.primary, display: "inline-block" }}></span> Cost</span>
                  <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ width: 10, height: 10, borderRadius: 2, background: BRAND.accent, display: "inline-block" }}></span> Covers</span>
                  <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ width: 10, height: 10, borderRadius: 2, background: BRAND.warning, display: "inline-block", opacity: 0.5 }}></span> Forecast</span>
                </div>
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={weeklyData} onClick={(d) => d && d.activeLabel && setActiveDay(d.activeLabel)}>
                  <CartesianGrid strokeDasharray="3 3" stroke={BRAND.border} />
                  <XAxis dataKey="day" tick={{ fill: BRAND.textMuted, fontSize: 12 }} axisLine={{ stroke: BRAND.border }} />
                  <YAxis yAxisId="cost" tick={{ fill: BRAND.textMuted, fontSize: 12 }} axisLine={false} tickFormatter={v => `$${v / 1000}k`} />
                  <YAxis yAxisId="covers" orientation="right" tick={{ fill: BRAND.textMuted, fontSize: 12 }} axisLine={false} />
                  <Tooltip contentStyle={{ background: BRAND.card, border: `1px solid ${BRAND.border}`, borderRadius: 8, color: BRAND.text, fontSize: 13 }} />
                  <Bar yAxisId="cost" dataKey="cost" fill={BRAND.primary} radius={[4, 4, 0, 0]} />
                  <Line yAxisId="covers" type="monotone" dataKey="covers" stroke={BRAND.accent} strokeWidth={2} dot={{ r: 4, fill: BRAND.accent }} />
                  <Line yAxisId="covers" type="monotone" dataKey="forecast" stroke={BRAND.warning} strokeWidth={1.5} strokeDasharray="5 5" dot={false} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Hourly staffing */}
            <div style={{ background: BRAND.card, borderRadius: 12, padding: 24, border: `1px solid ${BRAND.border}` }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
                <div>
                  <h3 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>Hourly staffing — {activeDay}</h3>
                  <p style={{ margin: "4px 0 0", fontSize: 13, color: BRAND.textMuted }}>Required vs rostered staff by hour</p>
                </div>
                <div style={{ display: "flex", gap: 4 }}>
                  {weeklyData.map(d => (
                    <button key={d.day} onClick={() => setActiveDay(d.day)} style={{ padding: "4px 10px", fontSize: 12, fontWeight: 500, borderRadius: 6, border: "none", cursor: "pointer", background: activeDay === d.day ? BRAND.primary : "transparent", color: activeDay === d.day ? "#fff" : BRAND.textMuted }}>{d.day}</button>
                  ))}
                </div>
              </div>
              <ResponsiveContainer width="100%" height={180}>
                <AreaChart data={hourlyStaffing}>
                  <CartesianGrid strokeDasharray="3 3" stroke={BRAND.border} />
                  <XAxis dataKey="hour" tick={{ fill: BRAND.textMuted, fontSize: 11 }} axisLine={{ stroke: BRAND.border }} />
                  <YAxis tick={{ fill: BRAND.textMuted, fontSize: 12 }} axisLine={false} />
                  <Tooltip contentStyle={{ background: BRAND.card, border: `1px solid ${BRAND.border}`, borderRadius: 8, color: BRAND.text, fontSize: 13 }} />
                  <Area type="monotone" dataKey="required" stroke={BRAND.primary} fill={`${BRAND.primary}33`} strokeWidth={2} />
                  <Area type="monotone" dataKey="actual" stroke={BRAND.accent} fill={`${BRAND.accent}22`} strokeWidth={2} strokeDasharray="4 4" />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {/* Employee table */}
            <div style={{ background: BRAND.card, borderRadius: 12, padding: 24, border: `1px solid ${BRAND.border}` }}>
              <h3 style={{ margin: "0 0 16px", fontSize: 16, fontWeight: 600 }}>Employee hours</h3>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                <thead>
                  <tr style={{ borderBottom: `1px solid ${BRAND.border}` }}>
                    {["Name", "Type", "Hours", "Shifts", "Cost", ""].map(h => (
                      <td key={h} style={{ padding: "8px 12px", color: BRAND.textMuted, fontWeight: 500, fontSize: 12 }}>{h}</td>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {employeeData.map((emp, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${BRAND.border}22` }}>
                      <td style={{ padding: "10px 12px", fontWeight: 500 }}>{emp.name}</td>
                      <td style={{ padding: "10px 12px" }}><TypeBadge type={emp.type} /></td>
                      <td style={{ padding: "10px 12px" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <div style={{ flex: 1, height: 4, background: BRAND.bg, borderRadius: 2, overflow: "hidden", maxWidth: 80 }}>
                            <div style={{ width: `${(emp.hours / emp.max) * 100}%`, height: "100%", background: emp.hours >= emp.max ? BRAND.warning : BRAND.primary, borderRadius: 2 }}></div>
                          </div>
                          <span style={{ fontSize: 12, color: BRAND.textMuted }}>{emp.hours}/{emp.max}h</span>
                        </div>
                      </td>
                      <td style={{ padding: "10px 12px", color: BRAND.textMuted }}>{emp.shifts}</td>
                      <td style={{ padding: "10px 12px", fontFamily: "monospace" }}>${emp.cost.toLocaleString()}</td>
                      <td style={{ padding: "10px 12px" }}>
                        {emp.status === "warning" && <AlertTriangle size={14} color={BRAND.warning} />}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Right sidebar */}
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            {/* Cost breakdown pie */}
            <div style={{ background: BRAND.card, borderRadius: 12, padding: 24, border: `1px solid ${BRAND.border}` }}>
              <h3 style={{ margin: "0 0 16px", fontSize: 16, fontWeight: 600 }}>Cost breakdown</h3>
              <div style={{ display: "flex", justifyContent: "center" }}>
                <PieChart width={200} height={200}>
                  <Pie data={costBreakdown} cx={100} cy={100} innerRadius={55} outerRadius={85} paddingAngle={3} dataKey="value">
                    {costBreakdown.map((entry, i) => <Cell key={i} fill={entry.color} />)}
                  </Pie>
                  <Tooltip contentStyle={{ background: BRAND.card, border: `1px solid ${BRAND.border}`, borderRadius: 8, color: BRAND.text, fontSize: 13 }} formatter={(v) => `$${v.toLocaleString()}`} />
                </PieChart>
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 8 }}>
                {costBreakdown.map((item, i) => (
                  <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 13 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <span style={{ width: 10, height: 10, borderRadius: 2, background: item.color, display: "inline-block" }}></span>
                      <span style={{ color: BRAND.textMuted }}>{item.name}</span>
                    </div>
                    <span style={{ fontFamily: "monospace", fontWeight: 500 }}>${item.value.toLocaleString()}</span>
                  </div>
                ))}
                <div style={{ borderTop: `1px solid ${BRAND.border}`, paddingTop: 8, display: "flex", justifyContent: "space-between", fontSize: 14, fontWeight: 600 }}>
                  <span>Total</span>
                  <span style={{ fontFamily: "monospace" }}>${costBreakdown.reduce((s, c) => s + c.value, 0).toLocaleString()}</span>
                </div>
              </div>
            </div>

            {/* Variance signals */}
            <div style={{ background: BRAND.card, borderRadius: 12, padding: 24, border: `1px solid ${BRAND.border}` }}>
              <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 600 }}>Demand signals</h3>
              <p style={{ margin: "0 0 16px", fontSize: 13, color: BRAND.textMuted }}>5-signal weighted variance engine</p>
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                {signals.map((s, i) => (
                  <div key={i} style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <div style={{ width: 32, height: 32, borderRadius: 8, background: `${BRAND.primary}22`, display: "flex", alignItems: "center", justifyContent: "center" }}>
                      <s.icon size={16} color={BRAND.primary} />
                    </div>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginBottom: 4 }}>
                        <span style={{ fontWeight: 500 }}>{s.name} <span style={{ color: BRAND.textMuted, fontWeight: 400 }}>({s.weight})</span></span>
                        <span style={{ fontFamily: "monospace", fontWeight: 600, color: s.value.startsWith("+") ? BRAND.accent : BRAND.warning }}>{s.value}</span>
                      </div>
                      <div style={{ height: 3, background: BRAND.bg, borderRadius: 2, overflow: "hidden" }}>
                        <div style={{ width: `${s.confidence * 100}%`, height: "100%", background: s.confidence > 0.8 ? BRAND.accent : BRAND.warning, borderRadius: 2 }}></div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ marginTop: 16, padding: "12px 16px", background: `${BRAND.accent}11`, borderRadius: 8, border: `1px solid ${BRAND.accent}33` }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: BRAND.accent, marginBottom: 2 }}>Net variance: +7.2%</div>
                <div style={{ fontSize: 12, color: BRAND.textMuted }}>Demand trending above baseline — roster is adjusted</div>
              </div>
            </div>

            {/* Alerts */}
            <div style={{ background: BRAND.card, borderRadius: 12, padding: 24, border: `1px solid ${BRAND.border}` }}>
              <h3 style={{ margin: "0 0 16px", fontSize: 16, fontWeight: 600 }}>Alerts</h3>
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                {alerts.map((a, i) => {
                  const colors = { success: BRAND.accent, warning: BRAND.warning, info: BRAND.primary };
                  return (
                    <div key={i} style={{ display: "flex", gap: 10, fontSize: 13 }}>
                      <a.icon size={16} color={colors[a.type]} style={{ marginTop: 2, flexShrink: 0 }} />
                      <div>
                        <div style={{ color: BRAND.text, lineHeight: 1.4 }}>{a.text}</div>
                        <div style={{ fontSize: 11, color: BRAND.textMuted, marginTop: 2 }}>{a.time}</div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
