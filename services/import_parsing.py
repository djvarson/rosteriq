"""
Shared parsing for the paste importers (staff list + roster exports).

Both importers take whatever a venue pastes out of Deputy / Tanda / Excel, so
they share the same tolerance rules: a UTF-8 BOM on the first header (Excel's
"CSV UTF-8"), the delimiter sniffed from the first real line, header aliases
matched with or without a parenthetical unit ("Break (mins)"), names built
from First + Last in preference to a display-name column, and a location
column that may cover more than one of the operator's venues.
"""

import csv
import difflib
import io
import re
from typing import Iterable, List, Optional, Tuple

BOM = "\ufeff"

# A column naming which of an operator's venues the row belongs to.
LOCATION_KEYS = {"location", "main location", "site", "venue", "business",
                 "workplace", "primary location", "home location", "location name"}


def read_rows(content: str) -> List[Tuple[int, List[str]]]:
    """Pasted text -> [(line_number, cells)], blank lines dropped. Line numbers
    are the paste's own (the line a row starts on, even when a quoted cell
    spans several), so a skipped row can be found in the source file."""
    text = (content or "").replace("\r\n", "\n").replace("\r", "\n").lstrip(BOM)
    first = next((ln for ln in text.split("\n") if ln.strip()), "")
    if not first:
        return []
    delim = "\t" if "\t" in first else (
        ";" if first.count(";") > first.count(",") else (
            "|" if "|" in first and "," not in first else ","))
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    out, prev = [], 0
    for row in reader:
        start, prev = prev + 1, reader.line_num
        if any(c.strip() for c in row):
            out.append((start, row))
    return out


def clean_header(h) -> str:
    return re.sub(r"\s+", " ", str(h or "").replace(BOM, "").strip())


def norm_header(h) -> str:
    return clean_header(h).lower()


def header_matches(h: str, keys) -> bool:
    """'Break (mins)' matches 'break' as well as 'break (mins)'."""
    if h in keys:
        return True
    bare = re.sub(r"\s*\([^)]*\)", "", h).strip()
    return bool(bare) and bare in keys


def map_columns(header: List[str], fields, multi=()) -> Tuple[dict, dict, List[str]]:
    """Map header cells to fields.

    fields: ordered [(field, alias_set)]; the first field whose aliases match a
    header wins it. A field in ``multi`` collects every matching column; the
    rest take the first. Returns (col, column_map, unmapped) where col is
    {field: index | [indexes]}, column_map is {header text: field} and
    unmapped lists the header texts nothing claimed."""
    col = {f: ([] if f in multi else None) for f, _ in fields}
    column_map, unmapped = {}, []
    for idx, raw in enumerate(header):
        h = norm_header(raw)
        if not h:
            continue
        claimed = None
        for field, keys in fields:
            if not header_matches(h, keys):
                continue
            if field in multi:
                col[field].append(idx)
                claimed = field
                break
            if col[field] is None:
                col[field] = idx
                claimed = field
                break
        if claimed:
            column_map[clean_header(raw)] = claimed
        else:
            unmapped.append(clean_header(raw))
    return col, column_map, unmapped


def norm_name(s) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def build_name(full: str = "", first: str = "", last: str = "") -> str:
    """First + Last when the export has them; else the full/display name, with
    a separate surname appended only when the name doesn't already end with
    it (a 'Name' column that holds only the first name)."""
    full = re.sub(r"\s+", " ", (full or "")).strip()
    first = re.sub(r"\s+", " ", (first or "")).strip()
    last = re.sub(r"\s+", " ", (last or "")).strip()
    if first or (last and not full):
        return f"{first} {last}".strip()
    if last and not norm_name(full).endswith(norm_name(last)):
        return f"{full} {last}".strip()
    return full


# --- Locations ---------------------------------------------------------------

_LOC_SPLIT = re.compile(r"\s*[;|,]\s*")
_LOC_NOISE = {"the", "hotel", "venue", "bar", "pub", "tavern", "inn", "at"}


def location_parts(cell: str) -> List[str]:
    return [p for p in _LOC_SPLIT.split((cell or "").strip()) if p]


def _loc_key(s: str) -> str:
    return norm_name(s)


def _loc_tokens(s: str) -> str:
    words = [w for w in re.findall(r"[a-z0-9]+", _loc_key(s)) if w not in _LOC_NOISE]
    return " ".join(words)


def _loc_score(candidate: str, venue_name: str) -> float:
    a, b = _loc_key(candidate), _loc_key(venue_name)
    if not a or not b:
        return 0.0
    if a == b:
        return 2.0
    ta, tb = _loc_tokens(candidate), _loc_tokens(venue_name)
    if ta and tb and ta == tb:
        return 1.5
    if a in b or b in a or (ta and tb and (ta in tb or tb in ta)):
        return 1.0
    return difflib.SequenceMatcher(None, ta or a, tb or b).ratio()


class LocationChoiceNeeded(ValueError):
    """The export covers several locations and none is clearly this venue."""

    def __init__(self, message: str, values: List[str]):
        super().__init__(message)
        self.values = values


def resolve_location(cells: Iterable[str], venue_name: str,
                     requested: Optional[str]) -> Tuple[bool, Optional[str], List[str]]:
    """Decide which location value in an export is this venue.

    Returns (filter_active, kept_value, distinct_cell_values). Filtering is
    only switched on when the column holds more than one distinct value, or
    when the manager named the location to keep. Raises LocationChoiceNeeded
    when it is on but no value can be chosen safely — importing another
    venue's people or shifts is worse than asking."""
    distinct, seen = [], set()
    parts, seen_parts = [], set()
    for c in cells:
        c = (c or "").strip()
        if not c:
            continue
        if _loc_key(c) not in seen:
            seen.add(_loc_key(c))
            distinct.append(c)
        for p in location_parts(c):
            if _loc_key(p) not in seen_parts:
                seen_parts.add(_loc_key(p))
                parts.append(p)
    requested = (requested or "").strip()
    if len(distinct) <= 1 and not requested:
        return False, None, distinct
    if not distinct:
        return False, None, distinct
    if requested:
        exact = [p for p in parts if _loc_key(p) == _loc_key(requested)]
        if not exact:
            exact = [c for c in distinct if _loc_key(c) == _loc_key(requested)]
        if not exact:
            loose = [p for p in parts
                     if _loc_key(requested) in _loc_key(p) or _loc_key(p) in _loc_key(requested)]
            exact = loose if len(loose) == 1 else []
        if not exact:
            raise LocationChoiceNeeded(
                f"No rows have location '{requested}'. Locations in this export: "
                f"{', '.join(parts)}.", parts)
        return True, exact[0], distinct
    scored = sorted(((_loc_score(p, venue_name), p) for p in parts), reverse=True)
    best_score, best = scored[0]
    tied = len(scored) > 1 and scored[1][0] == best_score
    if best_score < 0.6 or tied:
        raise LocationChoiceNeeded(
            f"This export covers more than one location ({', '.join(parts)}) and "
            f"none clearly matches '{venue_name}'. Choose the location to import "
            f"and paste again.", parts)
    return True, best, distinct


def location_matches(cell: str, kept: str) -> bool:
    """A row with no location is kept; a row naming several locations is kept
    when one of them is this venue (shared staff)."""
    cell = (cell or "").strip()
    if not cell:
        return True
    if _loc_key(cell) == _loc_key(kept):
        return True
    return any(_loc_key(p) == _loc_key(kept) for p in location_parts(cell))
