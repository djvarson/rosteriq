# Round 7 Track C: Tanda Webhook Events Feed Dashboard UI

## Insertion Point

**Location:** Insert the entire HTML markup (see section "HTML Markup" below) into `static/dashboard.html` **right before line 5130** (before the `<!-- Patterns Learned Panel -->` comment and the `<div class="patterns-learned role-l2"...` div).

**Context:** This places the new "Live Tanda Events" panel adjacent to the "Live Signal Feed" panel, with both feeding real-time operational data to L2 and OWNER users.

---

## HTML Markup

Add this block to `static/dashboard.html` at the insertion point specified above:

```html
            <!-- Live Tanda Events Feed (L2 + OWNER) -->
            <div class="tanda-events-feed role-l2" id="tandaEventsCard">
                <div class="tanda-events-header">
                    <div class="tanda-events-title-group">
                        <span class="tanda-events-pulse"></span>
                        <span class="tanda-events-title">Live Tanda Events</span>
                    </div>
                </div>
                <div class="tanda-events-filters">
                    <button class="tanda-filter-tab active" data-filter="all">All</button>
                    <button class="tanda-filter-tab" data-filter="shift">Roster</button>
                    <button class="tanda-filter-tab" data-filter="timesheet">Timesheet</button>
                    <button class="tanda-filter-tab" data-filter="leave">Leave</button>
                    <button class="tanda-filter-tab" data-filter="verify">Verify</button>
                </div>
                <div class="tanda-events-scroll" id="tandaEventsScroll">
                    <div class="tanda-events-empty">No events yet. Webhooks will appear here as they arrive.</div>
                </div>
            </div>
```

---

## CSS

Add this CSS block to the `<style>` section of `static/dashboard.html`. Place it after the "Live Signal Feed Card" section (after line ~1167) and before the "Patterns Learned Panel" section:

```css
        /* -------- Live Tanda Events Feed -------- */
        .tanda-events-feed {
            background-color: var(--card-bg);
            padding: 20px;
            border-radius: 12px;
            border: 1px solid var(--border-color);
            margin-bottom: 24px;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
        }

        .tanda-events-feed:hover {
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.07);
        }

        .tanda-events-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }

        .tanda-events-title-group {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .tanda-events-title {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-primary);
        }

        .tanda-events-pulse {
            display: inline-block;
            width: 8px;
            height: 8px;
            background-color: var(--info-blue);
            border-radius: 50%;
            animation: tanda-pulse-glow 1.5s ease-in-out infinite;
        }

        @keyframes tanda-pulse-glow {
            0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(14, 165, 233, 0.7); }
            50% { opacity: 0.8; box-shadow: 0 0 0 6px rgba(14, 165, 233, 0); }
        }

        .tanda-events-filters {
            display: flex;
            gap: 8px;
            margin-bottom: 16px;
            flex-wrap: wrap;
        }

        .tanda-filter-tab {
            padding: 6px 12px;
            border: 1px solid var(--border-color);
            background-color: transparent;
            border-radius: 6px;
            color: var(--text-secondary);
            font-size: 12px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s ease;
            white-space: nowrap;
        }

        .tanda-filter-tab:hover {
            border-color: var(--info-blue);
            color: var(--info-blue);
        }

        .tanda-filter-tab.active {
            background-color: var(--info-blue);
            border-color: var(--info-blue);
            color: #ffffff;
        }

        .tanda-events-scroll {
            display: flex;
            flex-direction: column;
            gap: 8px;
            max-height: 400px;
            overflow-y: auto;
        }

        .tanda-event-row {
            display: flex;
            gap: 12px;
            padding: 12px;
            background: var(--darker-bg);
            border-radius: 6px;
            border-left: 4px solid #ccc;
            font-size: 13px;
            transition: background 0.15s ease;
        }

        .tanda-event-row:hover {
            background: var(--border-color);
        }

        .tanda-event-row.info {
            border-left-color: var(--info-blue);
        }

        .tanda-event-row.warning {
            border-left-color: #f59e0b;
        }

        .tanda-event-row.critical {
            border-left-color: var(--alert-red);
        }

        .tanda-event-icon {
            flex-shrink: 0;
            width: 24px;
            height: 24px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 14px;
        }

        .tanda-event-content {
            flex: 1;
            min-width: 0;
        }

        .tanda-event-top-line {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 4px;
        }

        .tanda-event-org {
            font-weight: 600;
            color: var(--text-primary);
            font-size: 12px;
        }

        .tanda-event-type {
            display: inline-block;
            background: rgba(59, 130, 246, 0.1);
            color: #3b82f6;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 10px;
            font-weight: 600;
            text-transform: uppercase;
            white-space: nowrap;
        }

        .tanda-event-description {
            color: var(--text-primary);
            font-size: 13px;
            margin-bottom: 4px;
            word-break: break-word;
        }

        .tanda-event-time {
            font-size: 11px;
            color: var(--text-secondary);
        }

        .tanda-events-empty {
            text-align: center;
            padding: 20px;
            color: var(--text-secondary);
            font-size: 13px;
        }
```

---

## JavaScript

Add this JavaScript block to the `<script>` section of `static/dashboard.html`, at the end of the file before the closing `</script>` tag (after the `initLiveFeedWidget()` function and related code, around line 9070):

```javascript
        // ========== TANDA EVENTS FEED ==========
        window.TandaFeed = window.TandaFeed || {};

        let _tandaFeedIntervalId = null;
        let _tandaFeedDataCache = [];
        let _tandaFeedActiveFilter = 'all';

        /**
         * Format a timestamp as relative time (e.g., "2m ago")
         * @param {string|Date} timestamp - ISO 8601 or Date object
         * @returns {string}
         */
        window.TandaFeed.formatRelativeTime = function(timestamp) {
            const date = typeof timestamp === 'string' ? new Date(timestamp) : timestamp;
            const now = new Date();
            const seconds = Math.floor((now - date) / 1000);

            if (seconds < 60) return seconds + 's ago';
            const minutes = Math.floor(seconds / 60);
            if (minutes < 60) return minutes + 'm ago';
            const hours = Math.floor(minutes / 60);
            if (hours < 24) return hours + 'h ago';
            const days = Math.floor(hours / 24);
            return days + 'd ago';
        };

        /**
         * Get event icon and severity class based on event_type
         * @param {string} eventType - Tanda event type (e.g., "shift.published")
         * @returns {object} { icon: string, severity: 'info'|'warning'|'critical' }
         */
        window.TandaFeed.getEventMeta = function(eventType) {
            const meta = {
                'shift.published': { icon: '📋', severity: 'info' },
                'shift.updated': { icon: '✏️', severity: 'info' },
                'timesheet.approved': { icon: '✓', severity: 'info' },
                'timesheet.submitted': { icon: '📝', severity: 'info' },
                'leave.requested': { icon: '🏖️', severity: 'warning' },
                'leave.approved': { icon: '✓', severity: 'info' },
                'employee.updated': { icon: '👤', severity: 'info' },
                'verify.requested': { icon: '🔍', severity: 'warning' },
            };
            return meta[eventType] || { icon: '📌', severity: 'info' };
        };

        /**
         * Extract filter category from event_type
         * @param {string} eventType - Tanda event type
         * @returns {string} - filter category for grouping
         */
        window.TandaFeed.getFilterCategory = function(eventType) {
            if (eventType.startsWith('shift.')) return 'shift';
            if (eventType.startsWith('timesheet.')) return 'timesheet';
            if (eventType.startsWith('leave.')) return 'leave';
            if (eventType.startsWith('verify.')) return 'verify';
            return 'other';
        };

        /**
         * Generate human-readable description from event_type and data
         * @param {string} eventType - Tanda event type
         * @param {object} data - Event data object
         * @returns {string}
         */
        window.TandaFeed.getEventDescription = function(eventType, data) {
            const dataStr = JSON.stringify(data || {});

            switch (eventType) {
                case 'shift.published':
                    return 'Roster published' + (data?.shift_count ? ` — ${data.shift_count} shifts` : '');
                case 'shift.updated':
                    return 'Roster updated' + (data?.shift_count ? ` — ${data.shift_count} shifts` : '');
                case 'timesheet.approved':
                    return 'Timesheet approved' + (data?.employee_name ? ` — ${data.employee_name}` : '');
                case 'timesheet.submitted':
                    return 'Timesheet submitted' + (data?.employee_name ? ` — ${data.employee_name}` : '');
                case 'leave.requested':
                    return 'Leave request submitted' + (data?.employee_name ? ` — ${data.employee_name}` : '');
                case 'leave.approved':
                    return 'Leave approved' + (data?.employee_name ? ` — ${data.employee_name}` : '');
                case 'employee.updated':
                    return 'Employee updated' + (data?.employee_name ? ` — ${data.employee_name}` : '');
                case 'verify.requested':
                    return 'Verification requested' + (data?.field ? ` — ${data.field}` : '');
                default:
                    return eventType;
            }
        };

        /**
         * Render event rows into the feed container
         */
        function renderTandaEvents() {
            const container = document.getElementById('tandaEventsScroll');
            if (!container) return;

            // Filter events by active filter
            let filtered = _tandaFeedDataCache;
            if (_tandaFeedActiveFilter !== 'all') {
                filtered = _tandaFeedDataCache.filter(evt => 
                    window.TandaFeed.getFilterCategory(evt.event_type) === _tandaFeedActiveFilter
                );
            }

            // Render
            if (filtered.length === 0) {
                container.innerHTML = '<div class="tanda-events-empty">No events yet. Webhooks will appear here as they arrive.</div>';
                return;
            }

            container.innerHTML = filtered
                .map(evt => {
                    const meta = window.TandaFeed.getEventMeta(evt.event_type);
                    const relTime = window.TandaFeed.formatRelativeTime(evt.occurred_at);
                    const description = window.TandaFeed.getEventDescription(evt.event_type, evt.data);
                    const typeLabel = evt.event_type.split('.')[1] || evt.event_type;

                    return `
                        <div class="tanda-event-row ${meta.severity}">
                            <div class="tanda-event-icon">${meta.icon}</div>
                            <div class="tanda-event-content">
                                <div class="tanda-event-top-line">
                                    <span class="tanda-event-org">${evt.org_id}</span>
                                    <span class="tanda-event-type">${typeLabel}</span>
                                </div>
                                <div class="tanda-event-description">${description}</div>
                                <div class="tanda-event-time">${relTime}</div>
                            </div>
                        </div>
                    `;
                })
                .join('');
        }

        /**
         * Fetch events from the backend and update cache
         */
        async function loadTandaEvents() {
            const card = document.getElementById('tandaEventsCard');
            if (!card) return;

            try {
                // Get org_id from current context; for now, assume it's stored in a data attribute or global
                // If not available, gracefully skip
                const orgId = window._currentOrgId || card.dataset.orgId;
                if (!orgId) {
                    // L1 user or no org context; silently hide
                    card.style.display = 'none';
                    return;
                }

                const response = await fetch(
                    `/api/v1/tanda/webhook/events?org_id=${encodeURIComponent(orgId)}&limit=50`
                );

                if (response.status === 401 || response.status === 403) {
                    // Permission denied; silently hide
                    card.style.display = 'none';
                    return;
                }

                if (!response.ok) {
                    // Other errors; log but don't crash
                    console.warn(`Tanda events fetch failed: ${response.status}`);
                    return;
                }

                const body = await response.json();
                _tandaFeedDataCache = body.events || [];

                // Re-render with current filter
                renderTandaEvents();
            } catch (err) {
                console.warn('Error loading Tanda events:', err);
            }
        }

        /**
         * Initialize Tanda Events Feed with auto-refresh
         */
        function initTandaFeedWidget() {
            const card = document.getElementById('tandaEventsCard');
            if (!card) return;

            // Attach filter tab click handlers
            const filterTabs = card.querySelectorAll('.tanda-filter-tab');
            filterTabs.forEach(tab => {
                tab.addEventListener('click', function() {
                    // Update active state
                    filterTabs.forEach(t => t.classList.remove('active'));
                    this.classList.add('active');

                    // Update filter and re-render
                    _tandaFeedActiveFilter = this.dataset.filter;
                    renderTandaEvents();
                });
            });

            // Initial load
            loadTandaEvents();

            // Set up auto-refresh (20s), respecting visibility API
            if (_tandaFeedIntervalId) clearInterval(_tandaFeedIntervalId);

            _tandaFeedIntervalId = setInterval(() => {
                if (!document.hidden) {
                    loadTandaEvents();
                }
            }, 20 * 1000);

            // Resume/pause on visibility change
            document.addEventListener('visibilitychange', () => {
                if (!document.hidden && _tandaFeedIntervalId) {
                    loadTandaEvents();
                }
            });
        }

        // Initialize on DOM ready
        if (document.readyState !== 'loading') {
            initTandaFeedWidget();
        } else {
            document.addEventListener('DOMContentLoaded', initTandaFeedWidget);
        }
```

---

## API Endpoint Response Shape

The JavaScript assumes the backend returns this shape from `GET /api/v1/tanda/webhook/events?org_id={org_id}&limit={limit}`:

```json
{
    "org_id": "org_123",
    "count": 5,
    "events": [
        {
            "event_id": "uuid",
            "event_type": "shift.published",
            "org_id": "org_123",
            "occurred_at": "2026-04-16T14:30:00+00:00",
            "data": {
                "shift_count": 12,
                "employee_name": "Alice"
            }
        },
        ...
    ]
}
```

**Field assumptions made:**
- `event.event_type` – string, e.g. `"shift.published"`, `"timesheet.approved"`
- `event.org_id` – string, organization ID
- `event.occurred_at` – ISO 8601 datetime string
- `event.data` – object with arbitrary shape (may include `shift_count`, `employee_name`, `field`, etc.)

See `rosteriq/tanda_webhook_router.py` lines 31–39 for the `_tanda_event_to_dict()` function, which confirms these exact field names.

---

## Integration Notes

1. **Org ID Detection:** The JavaScript currently looks for `window._currentOrgId` (a global set by the dashboard on load) or `card.dataset.orgId`. You may need to populate one of these with the current user's org ID from the backend response.

2. **Role Visibility:** The HTML uses `class="role-l2"` to ensure the panel is only visible to L2 and OWNER users (matching the existing role-based CSS hiding rules in the dashboard).

3. **Auto-refresh:** Pauses when `document.hidden === true` (page in background tab) and resumes on visibility. Refresh interval is 20 seconds.

4. **Silent Failures:** If the user is L1 or lacks permission (401/403 response), the panel is hidden silently without error messages.

5. **Empty State:** Shows "No events yet. Webhooks will appear here as they arrive." when there are no events for the current filter.

6. **Filters:** Supported categories are `all`, `shift` (shift.* events), `timesheet` (timesheet.* events), `leave` (leave.* events), and `verify` (verify.* events).

---

## Metrics

- **HTML lines:** 21
- **CSS lines:** 140
- **JavaScript lines:** 250
- **Total spec lines:** ~450 (including markup structure and comments)
