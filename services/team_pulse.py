"""
Team pulse — the two staff-hub signals a manager needs on the morning screen:

  * procedure compliance: which SOP/JSP/policy documents still have staff who
    haven't acknowledged the CURRENT version (and who they are)
  * feed attention: staff posts on the team feed that no manager has replied to

One engine, three consumers — the daily briefing's attention list, the AI
copilot's tools, and (via the briefing) the dashboard — so the numbers can
never disagree. Pure read; never raises (a broken pillar must not take the
briefing down), returns zeros instead.
"""

from __future__ import annotations

from typing import Any


def procedure_compliance(db, venue_id: str) -> dict[str, Any]:
    """Outstanding acknowledgements across active, ack-required documents.

    Returns {"documents": n_active_requiring_ack, "outstanding_acks": total,
             "outstanding": [{"title", "version", "waiting_on": [names]}],
             "fully_acknowledged": [titles]}.
    """
    out = {"documents": 0, "outstanding_acks": 0, "outstanding": [], "fully_acknowledged": []}
    try:
        from rosteriq.routes.sops import _ack_stats
        docs = [d for d in (db.list_sop_documents(venue_id) or [])
                if d.get("active", True) and d.get("requires_ack", True)]
        if not docs:
            return out
        staff = db.get_employees(venue_id) or []
        acks = db.list_sop_acks(venue_id) or []
        for d in docs:
            st = _ack_stats(d, staff, acks)
            out["documents"] += 1
            waiting = st.get("outstanding_names") or []
            if waiting:
                out["outstanding_acks"] += len(waiting)
                out["outstanding"].append({
                    "document_id": d["id"], "title": d.get("title"),
                    "version": int(d.get("version") or 1),
                    "acknowledged": st.get("acknowledged_current_version", 0),
                    "required": st.get("required", 0),
                    "waiting_on": waiting,
                })
            elif st.get("required"):
                out["fully_acknowledged"].append(d.get("title"))
        # Worst first: most people waiting
        out["outstanding"].sort(key=lambda x: -len(x["waiting_on"]))
    except Exception:
        pass
    return out


def feed_attention(db, venue_id: str) -> dict[str, Any]:
    """Staff posts nobody from management has replied to yet (newest first).

    A staff post counts as answered once any comment on it was written by a
    manager. Pinned/manager posts are never 'awaiting'.
    Returns {"awaiting_reply": n, "posts": [{"id","author","body","age_comments"}]}.
    """
    out = {"awaiting_reply": 0, "posts": []}
    try:
        posts = db.list_feed_posts(venue_id, limit=100) or []
        for p in posts:
            if p.get("removed") or p.get("author_role") != "staff":
                continue
            comments = p.get("comments") or []
            # Comments record the commenter's role at write time; a colleague's
            # reply is not an answer from management.
            answered = any(c.get("author_role") == "manager" for c in comments)
            if answered:
                continue
            body = str(p.get("body") or "")
            out["posts"].append({
                "id": p.get("id"),
                "author": p.get("author_name"),
                "body": body if len(body) <= 140 else body[:137] + "...",
                "comments": len(comments),
                "created_at": str(p.get("created_at")),
            })
        out["awaiting_reply"] = len(out["posts"])
    except Exception:
        pass
    return out
