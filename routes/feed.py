"""
Team feed — the two-way sibling of Announcements.

Announcements (routes/comms.py) stay the manager broadcast channel. The feed
is where the whole team talks: staff AND managers post, anyone at the venue
can comment and react, and managers can pin or remove.

Routes (all under /api/feed):
    POST   /api/feed/posts                 -- post to a venue feed
    GET    /api/feed/posts?venue_id=       -- the venue's feed (pinned first)
    POST   /api/feed/posts/{id}/comments   -- append a comment
    POST   /api/feed/posts/{id}/react      -- toggle an emoji reaction
    POST   /api/feed/posts/{id}/pin        -- manager only: pin / unpin
    DELETE /api/feed/posts/{id}            -- author or manager: soft-remove

Identity: the same email-linked staff identity as the rest of the staff
portal. A caller who resolves to an employee record at the post's venue acts
as ``staff`` (author_name = the employee's name); otherwise the caller must
be a manager/owner with access to the venue and acts as ``manager``.
Cross-venue posts are a 404 — we never confirm another venue's post exists.
"""

import logging
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.routes.staff_portal import _linked_employee, _no_link_response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/feed", tags=["feed"])

# The five reactions the UI offers. Literal unicode strings.
ALLOWED_REACTIONS = ("\U0001F44D", "❤️", "\U0001F389", "\U0001F44F", "\U0001F602")
# Also accept the bare red heart (no variation selector) and normalise it.
_HEART_BARE = "❤"

# Roles that count as "manager" for pin/remove and for posting as management.
MANAGER_ROLES = ("owner", "manager", "admin")


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class PostBody(BaseModel):
    venue_id: str
    body: str = Field(..., min_length=1, max_length=2000)


class CommentBody(BaseModel):
    body: str = Field(..., min_length=1, max_length=1000)


class ReactBody(BaseModel):
    emoji: str = Field(..., min_length=1, max_length=8)


class PinBody(BaseModel):
    pinned: bool


# ---------------------------------------------------------------------------
# Caller resolution
# ---------------------------------------------------------------------------

class _Actor:
    __slots__ = ("role", "name", "employee_id")

    def __init__(self, role: str, name: str, employee_id: Optional[str] = None):
        self.role = role
        self.name = name
        self.employee_id = employee_id

    @property
    def is_manager(self) -> bool:
        return self.role == "manager"


def _is_manager_user(user: UserContext) -> bool:
    return (user.role or "").lower() in MANAGER_ROLES


def _resolve_actor(db, user: UserContext, venue_id: str, *, hide_as_404: bool = False) -> _Actor:
    """Who is the caller AT THIS VENUE?

    * Linked staff at ``venue_id``  -> staff actor (employee name).
    * Manager/owner with venue access -> manager actor (user name/email).
    * Anyone else -> 403 (or 404 when ``hide_as_404`` — per-post routes must
      not reveal that another venue's post exists).
    """
    emp, emp_vid = _linked_employee(db, user)
    if emp is not None and str(emp_vid) == str(venue_id):
        return _Actor("staff", emp.name, employee_id=emp.id)

    try:
        enforce_venue_access(venue_id)
    except HTTPException:
        if hide_as_404:
            raise HTTPException(status_code=404, detail="Post not found")
        raise

    if _is_manager_user(user):
        return _Actor("manager", user.name or user.email)

    # Has venue access but is neither a linked staff member nor a manager —
    # tell them exactly how to fix it (same message the staff portal uses).
    raise HTTPException(status_code=403, detail=_no_link_response(user)["message"])


def _load_post(db, user: UserContext, post_id: str):
    """Fetch a live post the caller may see; returns (post, actor)."""
    post = db.get_feed_post(post_id)
    if not post or post.get("removed"):
        raise HTTPException(status_code=404, detail="Post not found")
    actor = _resolve_actor(db, user, post.get("venue_id"), hide_as_404=True)
    return post, actor


# ---------------------------------------------------------------------------
# Payload shaping
# ---------------------------------------------------------------------------

def _reaction_summary(post: dict, user_id: str) -> tuple[dict, list]:
    reactions = post.get("reactions") or {}
    if not isinstance(reactions, dict):
        reactions = {}
    counts = {}
    mine = []
    for emoji, ids in reactions.items():
        ids = list(ids or [])
        if not ids:
            continue
        counts[emoji] = len(ids)
        if user_id in ids:
            mine.append(emoji)
    return counts, mine


def _post_payload(post: dict, user_id: str) -> dict:
    counts, mine = _reaction_summary(post, user_id)
    comments = []
    for c in list(post.get("comments") or []):
        c = dict(c)
        c["created_at"] = str(c.get("created_at"))
        comments.append(c)
    return {
        "id": post["id"],
        "venue_id": post.get("venue_id"),
        "author_user_id": post.get("author_user_id"),
        "author_name": post.get("author_name"),
        "author_role": post.get("author_role"),
        "body": post.get("body"),
        "pinned": bool(post.get("pinned")),
        "removed": bool(post.get("removed")),
        "reactions": {k: list(v or []) for k, v in (post.get("reactions") or {}).items()},
        "reaction_counts": counts,
        "my_reactions": mine,
        "comments": comments,
        "comment_count": len(comments),
        "created_at": str(post.get("created_at")),
        "updated_at": str(post.get("updated_at") or post.get("created_at")),
    }


def _normalise_emoji(raw: str) -> str:
    e = (raw or "").strip()
    if e == _HEART_BARE:
        e = "❤️"
    if e not in ALLOWED_REACTIONS:
        raise HTTPException(
            status_code=422,
            detail="Reaction must be one of: " + " ".join(ALLOWED_REACTIONS),
        )
    return e


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/posts")
async def create_post(body: PostBody,
                      user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    actor = _resolve_actor(db, user, body.venue_id)
    now = datetime.utcnow()
    post = {
        "id": f"fp-{uuid.uuid4().hex[:10]}",
        "venue_id": body.venue_id,
        "author_user_id": user.user_id,
        "author_name": actor.name,
        "author_role": actor.role,
        "body": body.body.strip(),
        "pinned": False,
        "removed": False,
        "reactions": {},
        "comments": [],
        "created_at": now,
        "updated_at": now,
    }
    db.save_feed_post(post)
    logger.info(f"Feed post {post['id']} at {body.venue_id} by {actor.role} {actor.name!r}")
    return _post_payload(post, user.user_id)


@router.get("/posts")
async def list_posts(venue_id: str = Query(...),
                     limit: int = Query(50, ge=1, le=200),
                     user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    _resolve_actor(db, user, venue_id)
    rows = db.list_feed_posts(venue_id, limit=limit) or []
    posts = [_post_payload(p, user.user_id) for p in rows if not p.get("removed")]
    return {"venue_id": venue_id, "count": len(posts), "posts": posts}


@router.post("/posts/{post_id}/comments")
async def add_comment(post_id: str, body: CommentBody,
                      user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    post, actor = _load_post(db, user, post_id)
    comments = list(post.get("comments") or [])
    comments.append({
        "id": f"fc-{uuid.uuid4().hex[:10]}",
        "author_user_id": user.user_id,
        "author_name": actor.name,
        "author_role": actor.role,
        "body": body.body.strip(),
        "created_at": datetime.utcnow(),
    })
    post["comments"] = comments
    post["updated_at"] = datetime.utcnow()
    db.save_feed_post(post)
    return _post_payload(post, user.user_id)


@router.post("/posts/{post_id}/react")
async def toggle_reaction(post_id: str, body: ReactBody,
                          user: UserContext = Depends(get_current_user)) -> dict:
    emoji = _normalise_emoji(body.emoji)
    db = get_db()
    post, _actor = _load_post(db, user, post_id)
    reactions = dict(post.get("reactions") or {})
    ids = list(reactions.get(emoji) or [])
    if user.user_id in ids:
        ids = [i for i in ids if i != user.user_id]
        state = "removed"
    else:
        ids.append(user.user_id)
        state = "added"
    if ids:
        reactions[emoji] = ids
    else:
        reactions.pop(emoji, None)
    post["reactions"] = reactions
    post["updated_at"] = datetime.utcnow()
    db.save_feed_post(post)
    counts, mine = _reaction_summary(post, user.user_id)
    return {"post_id": post_id, "emoji": emoji, "status": state,
            "reaction_counts": counts, "my_reactions": mine}


@router.post("/posts/{post_id}/pin")
async def pin_post(post_id: str, body: PinBody,
                   user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    post, actor = _load_post(db, user, post_id)
    if not (actor.is_manager or _is_manager_user(user)):
        raise HTTPException(status_code=403,
                            detail="Only managers can pin or unpin feed posts")
    post["pinned"] = bool(body.pinned)
    post["updated_at"] = datetime.utcnow()
    db.save_feed_post(post)
    return {"status": "pinned" if body.pinned else "unpinned", "post_id": post_id,
            "post": _post_payload(post, user.user_id)}


@router.delete("/posts/{post_id}")
async def remove_post(post_id: str,
                      user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    post, actor = _load_post(db, user, post_id)
    is_author = str(post.get("author_user_id") or "") == str(user.user_id)
    if not (is_author or actor.is_manager or _is_manager_user(user)):
        raise HTTPException(status_code=403,
                            detail="Only the author or a manager can remove this post")
    post["removed"] = True
    post["updated_at"] = datetime.utcnow()
    db.save_feed_post(post)
    return {"status": "removed", "post_id": post_id}
