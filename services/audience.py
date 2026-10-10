"""
Who a notice or procedure is for. One rule, shared by announcements
(routes/comms.py) and procedures (routes/sops.py), and the pills managers
pick from.

An audience is a list of tokens; empty means everyone.
* An area or skill token ("front bar", "Kitchen") reaches people who hold that
  skill (models.holds_skill — case-insensitive, normalised).
* A group token "all <word>" ("all bar") reaches people holding ANY skill
  with that word in it — Front Bar, Sports Bar and Bar Back for "all bar",
  but not Barista.

The pills come from the venue's areas (services/venue_areas), the distinct
skills its current staff hold, and a group pill for each word that two or
more of those names share.
"""

from __future__ import annotations

from typing import Iterable, Optional

from rosteriq.models import holds_skill, normalise_area, normalise_skills

GROUP_PREFIX = "all "
_NOT_GROUP_WORDS = {"and", "the", "of", "&", "-", "staff", "team"}


def _words(name: str) -> set:
    return set(normalise_area(name).split())


def group_word(token) -> Optional[str]:
    """'all bar' -> 'bar'; None for an ordinary area/skill token."""
    t = normalise_area(token)
    if t.startswith(GROUP_PREFIX) and len(t) > len(GROUP_PREFIX):
        return t[len(GROUP_PREFIX):]
    return None


def token_matches(token, skills) -> bool:
    word = group_word(token)
    if word is None:
        return holds_skill(skills, token)
    wanted = set(word.split())
    return any(wanted <= _words(s) for s in normalise_skills(skills))


def matches(audience, emp) -> bool:
    """Is ``emp`` (None = nobody in particular) addressed by ``audience``?"""
    tokens = [t for t in (audience or []) if normalise_area(t)]
    if not tokens:
        return True
    skills = getattr(emp, "skills", None) or []
    return any(token_matches(t, skills) for t in tokens)


def group_suggestion(token, staff: Iterable) -> Optional[str]:
    """'all bar' when ``token`` ('bar') reaches nobody exactly but its group
    would — the hint a manager needs after a 'nobody works bar' refusal."""
    if group_word(token) is not None:
        return None
    group = GROUP_PREFIX + normalise_area(token)
    if any(token_matches(group, getattr(e, "skills", None) or []) for e in staff):
        return group
    return None


def options(venue, staff: Iterable) -> list:
    """Pills for the composer: [{value, label, kind: area|skill|group, people}]."""
    from rosteriq.services.venue_areas import venue_areas

    staff = list(staff or [])
    seen, out = set(), []

    def people(token):
        return sum(1 for e in staff if token_matches(token, getattr(e, "skills", None) or []))

    for a in venue_areas(venue) if venue is not None else []:
        if a["name"] not in seen:
            seen.add(a["name"])
            out.append({"value": a["name"], "label": a["label"], "kind": "area",
                        "people": people(a["name"])})
    for e in staff:
        for s in normalise_skills(getattr(e, "skills", None) or []):
            if s not in seen:
                seen.add(s)
                out.append({"value": s, "label": s.title(), "kind": "skill", "people": people(s)})

    word_count: dict = {}
    for name in seen:
        for w in _words(name):
            if w not in _NOT_GROUP_WORDS and len(w) > 1:
                word_count[w] = word_count.get(w, 0) + 1
    groups = []
    for w, n in word_count.items():
        if n >= 2:
            token = GROUP_PREFIX + w
            groups.append({"value": token, "label": f"All {w}", "kind": "group",
                           "people": people(token)})
    groups.sort(key=lambda g: g["value"])
    return groups + out
