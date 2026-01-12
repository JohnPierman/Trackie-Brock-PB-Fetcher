#!/usr/bin/env python3
"""
Scrape Trackie U SPORTS athlete pages and compute PBs for the last N seasons.

Note: This was made with Cursor, and wasn't refined extensively. Use at your own risk.

Example:
  python3 scrape_trackie_pbs.py --years-back 5 --out brock_pbs.csv
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    tqdm = None  # type: ignore


DEFAULT_UNIVERSITY_URL = "https://www.trackie.com/usports/tnf/universities/brock-university/3/"


@dataclasses.dataclass(frozen=True)
class PerformanceRow:
    season: str  # e.g. "2025/26"
    event: str
    perf_raw: str
    meet: str
    meet_url: Optional[str]
    date_raw: str
    date: Optional[dt.date]


@dataclasses.dataclass(frozen=True)
class ParsedPerf:
    value: float
    unit: str  # "s" | "m" | "pts"
    better_is: str  # "lower" | "higher"


class RateLimiter:
    """Global minimum interval between HTTP requests across threads."""

    def __init__(self, min_interval_seconds: float) -> None:
        self._min_interval = max(0.0, float(min_interval_seconds))
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            now = time.time()
            sleep_for = self._min_interval - (now - self._last)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last = time.time()


class TrackieClient:
    def __init__(
        self,
        delay_seconds: float = 0.6,
        timeout_seconds: float = 30.0,
        retries: int = 3,
        user_agent: str = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
    ) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-CA,en;q=0.9",
                "Connection": "keep-alive",
            }
        )
        self._timeout = float(timeout_seconds)
        self._retries = max(0, int(retries))
        self._rate = RateLimiter(delay_seconds)

    def get_html(self, url: str) -> str:
        last_exc: Optional[BaseException] = None
        for attempt in range(self._retries + 1):
            try:
                self._rate.wait()
                resp = self._session.get(url, timeout=self._timeout)
                resp.raise_for_status()
                return resp.text
            except BaseException as e:
                last_exc = e
                if attempt >= self._retries:
                    break
                # simple exponential backoff
                time.sleep(min(8.0, 0.7 * (2**attempt)))
        assert last_exc is not None
        raise RuntimeError(f"Failed to fetch {url}: {last_exc}") from last_exc


ATHLETE_HREF_RE = re.compile(r"^/usports/tnf/athletes/[^/]+/\d+/?$")
SEASON_RE = re.compile(r"\b(20\d{2})/(\d{2})\b")


def _normalize_ws(s: str) -> str:
    return re.sub(r"\\s+", " ", (s or "").replace("\\xa0", " ")).strip()


def _abs_url(base: str, maybe_relative: Optional[str]) -> Optional[str]:
    if not maybe_relative:
        return None
    return urljoin(base, maybe_relative)


def parse_university_athlete_urls(university_html: str, university_url: str) -> list[str]:
    soup = BeautifulSoup(university_html, "lxml")
    urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        if ATHLETE_HREF_RE.match(href):
            urls.add(_abs_url(university_url, href) or href)
    return sorted(urls)


def parse_university_athlete_index(university_html: str, university_url: str) -> dict[str, dict]:
    """
    Build an index of athlete_url -> metadata from the university roster page.

    Trackie typically includes sex on the roster table, but not consistently on athlete profile pages.
    """
    soup = BeautifulSoup(university_html, "lxml")
    index: dict[str, dict] = {}

    # Prefer the Athletes table (h4 "Athletes" -> following table)
    athletes_heading = None
    for h4 in soup.find_all("h4"):
        if _normalize_ws(h4.get_text(" ")).lower() == "athletes":
            athletes_heading = h4
            break

    candidate_tables = []
    if athletes_heading is not None:
        t = athletes_heading.find_next("table")
        if t is not None:
            candidate_tables.append(t)
    else:
        # Fallback: scan all tables
        candidate_tables = soup.find_all("table")

    for table in candidate_tables:
        for tr in table.find_all("tr"):
            a = tr.find("a", href=True)
            if not a:
                continue
            href = str(a.get("href") or "").strip()
            if not ATHLETE_HREF_RE.match(href):
                continue
            athlete_url = _abs_url(university_url, href) or href

            tds = tr.find_all("td")
            sex = ""
            if len(tds) >= 2:
                sex_candidate = _normalize_ws(tds[1].get_text(" "))
                if sex_candidate in {"M", "F"}:
                    sex = sex_candidate

            name = _normalize_ws(a.get_text(" "))
            index[athlete_url] = {
                "name": name,
                "sex": sex,
            }

    # Ensure we include everyone even if we couldn't parse the roster rows
    for athlete_url in parse_university_athlete_urls(university_html, university_url):
        index.setdefault(athlete_url, {"name": "", "sex": ""})

    return index


def _infer_current_season_end_year(now: Optional[dt.date] = None) -> int:
    """
    Trackie seasons are typically displayed as YYYY/YY (Aug -> Jul).
    - Aug-Dec 2025 -> season end year 2026 (2025/26)
    - Jan-Jul 2026 -> season end year 2026 (2025/26)
    """
    now = now or dt.date.today()
    return now.year + 1 if now.month >= 8 else now.year


def _season_end_years(years_back: int, now: Optional[dt.date] = None) -> list[int]:
    end = _infer_current_season_end_year(now)
    n = max(1, int(years_back))
    return list(range(end, end - n, -1))


def _infer_sex_from_heading(h: str) -> Optional[str]:
    s = _normalize_ws(h).lower()
    if s.startswith("men"):
        return "M"
    if s.startswith("women"):
        return "F"
    return None


def parse_rankings_athlete_index(rankings_html: str, base_url: str) -> dict[str, dict]:
    """
    Rankings pages include Men’s/Women’s event sections. We infer athlete sex based on the
    event heading that contains the athlete link.
    """
    soup = BeautifulSoup(rankings_html, "lxml")
    index: dict[str, dict] = {}

    for h4 in soup.find_all("h4"):
        sex = _infer_sex_from_heading(h4.get_text(" "))
        if not sex:
            continue

        # Collect links until the next h4.
        for el in h4.next_elements:
            if getattr(el, "name", None) == "h4" and el is not h4:
                break
            if getattr(el, "name", None) != "a":
                continue
            href = el.get("href")
            if not href:
                continue
            href = str(href).strip()
            if not ATHLETE_HREF_RE.match(href):
                continue
            athlete_url = _abs_url(base_url, href) or href
            name = _normalize_ws(el.get_text(" "))
            prev = index.get(athlete_url, {})
            index[athlete_url] = {
                "name": prev.get("name") or name,
                "sex": prev.get("sex") or sex,
            }

    return index


def _normalize_rankings_event_name(heading_text: str) -> str:
    """
    Rankings headings look like:
      "Men’s 60 Meter", "Women’s Long Jump (non ranking)"
    We normalize to:
      "60 Meter", "Long Jump"
    """
    s = _normalize_ws(heading_text)
    s = re.sub(r"^(Men’s|Men's|Wom[e]?n’s|Women's)\s+", "", s, flags=re.I).strip()
    s = re.sub(r"\s*\(non ranking\)\s*$", "", s, flags=re.I).strip()
    return s


def canonical_event_name(event: str) -> str:
    """
    Normalize event names so we don't create duplicates like:
      "Men’s 60 Meter", "Women’s 60 Meter", "60 Meter"

    This is applied to BOTH athlete profile tables and rankings-derived events.
    """
    s = _normalize_ws(event)
    if not s:
        return s

    # Normalize apostrophes (and common mojibake) so prefix stripping is reliable.
    # Trackie often uses U+2019 RIGHT SINGLE QUOTATION MARK.
    s = s.replace("â€™", "’")  # just in case of mojibake
    s = s.replace("’", "'").replace("‘", "'")

    # Strip common gender prefixes
    s = re.sub(
        r"^(men'?s|mens|men|women'?s|womens|women)\s+",
        "",
        s,
        flags=re.I,
    ).strip()

    # Strip common suffix annotations
    s = re.sub(r"\s*\(non ranking\)\s*$", "", s, flags=re.I).strip()

    # Normalize common meter shorthand: "60m" -> "60 Meter", "4x400m" -> "4x400 Meter"
    s = re.sub(r"\\b(\\d{2,4})\\s*m\\b", r"\\1 Meter", s, flags=re.I)
    s = re.sub(r"\\b(4x\\d{2,4})\\s*m\\b", r"\\1 Meter", s, flags=re.I)
    # Also normalize British spellings and variations
    s = re.sub(r"\\b(\\d{2,4})\\s*(metre|metres)\\b", r"\\1 Meter", s, flags=re.I)

    # Title-ish normalize spacing
    s = _normalize_ws(s)
    return s


def _infer_better_is_from_event_name(event: str) -> str:
    e = event.lower()
    # Field events (higher is better)
    if any(
        k in e
        for k in [
            "jump",
            "throw",
            "shot put",
            "discus",
            "javelin",
            "hammer",
            "weight throw",
            "pole vault",
            "high jump",
            "long jump",
            "triple jump",
        ]
    ):
        return "higher"
    # Combined events (higher is better)
    if "athlon" in e or "pentathlon" in e or "heptathlon" in e or "decathlon" in e:
        return "higher"
    # Default to track/time (lower is better)
    return "lower"


def parse_rankings_entries(rankings_html: str, base_url: str) -> list[dict]:
    """
    Parse season-best performances from a rankings page.

    NOTE: Rankings pages generally do NOT include meet/date; only rank + athlete + perf.
    """
    soup = BeautifulSoup(rankings_html, "lxml")
    entries: list[dict] = []

    for h4 in soup.find_all("h4"):
        sex = _infer_sex_from_heading(h4.get_text(" "))
        if not sex:
            continue
        event = canonical_event_name(_normalize_rankings_event_name(h4.get_text(" ")))
        table = h4.find_next("table")
        if not table:
            continue
        for tr in table.find_all("tr"):
            a = tr.find("a", href=True)
            if not a:
                continue
            href = str(a.get("href") or "").strip()
            if not ATHLETE_HREF_RE.match(href):
                continue
            athlete_url = _abs_url(base_url, href) or href
            athlete_name = _normalize_ws(a.get_text(" "))
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            perf_td = tds[-1]
            perf_raw = _normalize_ws(perf_td.get_text(" "))
            perf_title = perf_td.get("title") or perf_td.get("data-original-title") or perf_td.get("data-title")
            if perf_title:
                perf_title = _normalize_ws(perf_title)
                # If tooltip exists and contains a time/performance value, prefer it
                if perf_title and perf_title != perf_raw:
                    # Check if tooltip looks like a performance (has numbers)
                    if re.search(r"\d", perf_title):
                        perf_raw = perf_title
            if not perf_raw or perf_raw.upper() in {"DNS", "DNF", "DQ", "NM", "SCR"}:
                continue
            entries.append(
                {
                    "athlete_url": athlete_url,
                    "athlete_name": athlete_name,
                    "sex": sex,
                    "event": event,
                    "perf_raw": perf_raw,
                }
            )
    return entries


def _season_years(season: str) -> Optional[tuple[int, int]]:
    m = SEASON_RE.search(season)
    if not m:
        return None
    start = int(m.group(1))
    end = (start // 100) * 100 + int(m.group(2))  # 2025/26 -> 2026
    return start, end


MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def infer_date_from_season(season: str, date_raw: str) -> Optional[dt.date]:
    years = _season_years(season)
    if not years:
        return None
    start_year, end_year = years
    s = _normalize_ws(date_raw).lower()
    m = re.search(r"([a-zA-Z]{3,})\s+(\d{1,2})", s)
    if not m:
        return None
    month_s = m.group(1).lower()
    day = int(m.group(2))
    month = MONTHS.get(month_s)
    if not month:
        month = MONTHS.get(month_s[:3])
    if not month:
        return None
    # Aug-Dec are assumed in the first season year; Jan-Jul in the second.
    year = start_year if month >= 8 else end_year
    try:
        return dt.date(year, month, day)
    except ValueError:
        return None


def parse_athlete_name(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find(["h1", "h2"])
    if not h1:
        return None
    name = _normalize_ws(h1.get_text(" "))
    return name or None


def parse_athlete_sex(soup: BeautifulSoup) -> Optional[str]:
    # best-effort: find a table row whose header/cell is "Sex"
    for tag in soup.find_all(["th", "td"]):
        if _normalize_ws(tag.get_text(" ")).lower() == "sex":
            nxt = tag.find_next(["td", "th"])
            if nxt:
                val = _normalize_ws(nxt.get_text(" "))
                if val in {"M", "F"}:
                    return val
    # fallback: common patterns in profile summary
    txt = _normalize_ws(soup.get_text(" ")).lower()
    if re.search(r"\bsex\s*m\b", txt):
        return "M"
    if re.search(r"\bsex\s*f\b", txt):
        return "F"
    return None


def parse_performance_rows(athlete_html: str, athlete_url: str) -> list[PerformanceRow]:
    soup = BeautifulSoup(athlete_html, "lxml")
    rows: list[PerformanceRow] = []

    # Find any heading containing "Performance" and a season like 2025/26, then read the following table.
    for heading in soup.find_all(re.compile("^h[1-6]$")):
        heading_text = _normalize_ws(heading.get_text(" "))
        if "performance" not in heading_text.lower():
            continue
        season_match = SEASON_RE.search(heading_text)
        if not season_match:
            continue
        season = season_match.group(0)

        table = heading.find_next("table")
        if not table:
            continue

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            # Issue #1: Skip header rows (rows where first cell looks like a header, e.g., "Event", "Performance")
            first_cell_text = _normalize_ws(tds[0].get_text(" ")).lower()
            if first_cell_text in ["event", "performance", "perf", "meet", "date"]:
                continue
            event = canonical_event_name(_normalize_ws(tds[0].get_text(" ")))
            
            # Issue #3: Extract raw time from tooltip if available (title attribute)
            perf_td = tds[1]
            perf_raw = _normalize_ws(perf_td.get_text(" "))
            # Check for title attribute which often contains the raw unconverted time
            perf_title = perf_td.get("title") or perf_td.get("data-original-title") or perf_td.get("data-title")
            if perf_title:
                perf_title = _normalize_ws(perf_title)
                # If tooltip exists and contains a time/performance value, prefer it
                if perf_title and perf_title != perf_raw:
                    # Check if tooltip looks like a performance (has numbers)
                    if re.search(r"\d", perf_title):
                        perf_raw = perf_title
            
            meet_td = tds[2]
            meet = _normalize_ws(meet_td.get_text(" "))
            meet_a = meet_td.find("a", href=True)
            meet_url = _abs_url(athlete_url, str(meet_a.get("href"))) if meet_a else None
            date_raw = _normalize_ws(tds[3].get_text(" "))
            date = infer_date_from_season(season, date_raw)

            if not event or not perf_raw:
                continue
            if perf_raw.upper() in {"DNS", "DNF", "DQ", "NM", "SCR"}:
                continue

            rows.append(
                PerformanceRow(
                    season=season,
                    event=event,
                    perf_raw=perf_raw,
                    meet=meet,
                    meet_url=meet_url,
                    date_raw=date_raw,
                    date=date,
                )
            )

    return rows


def parse_perf(perf_raw: str, event: str) -> Optional[ParsedPerf]:
    s = _normalize_ws(perf_raw)
    # drop wind / notes in parentheses e.g. "10.65 (+1.2)"
    s = re.sub(r"\\s*\\([^)]*\\)\\s*", " ", s).strip()
    # drop common footnote markers like "*1", "^5" etc
    s = re.sub(r"[*^]\\s*\\d+\\b", "", s).strip()
    s = s.replace("*", "").replace("^", "").strip()

    if not s or any(tok in s.upper() for tok in ["DNS", "DNF", "DQ", "NM", "SCR"]):
        return None

    s_lower = s.lower()
    event_lower = event.lower()

    def _first_number(seg: str) -> Optional[float]:
        m = re.search(r"(-?\\d+(?:\\.\\d+)?)", seg)
        return float(m.group(1)) if m else None

    is_field_event = _infer_better_is_from_event_name(event) == "higher" and any(
        k in event_lower for k in [
            "jump", "throw", "shot put", "discus", "javelin", "hammer",
            "weight throw", "pole vault", "high jump", "long jump", "triple jump"
        ]
    )

    if "pts" in s_lower or "point" in s_lower or "athlon" in event_lower:
        val = _first_number(s_lower)
        if val is None:
            return None
        return ParsedPerf(value=val, unit="pts", better_is="higher")

    if "cm" in s_lower:
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*cm", s_lower)
        if not m:
            return None
        return ParsedPerf(value=float(m.group(1)) / 100.0, unit="m", better_is="higher")

    if is_field_event:
        # Check for meters first
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*m\b", s_lower)
        if m:
            return ParsedPerf(value=float(m.group(1)), unit="m", better_is="higher")
        # Check for centimeters
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*cm\b", s_lower)
        if m:
            return ParsedPerf(value=float(m.group(1)) / 100.0, unit="m", better_is="higher")
        # If it's a field event but no unit found, check if it's just a number (assume meters)
        m = re.search(r"(-?\d+(?:\.\d+)?)", s_lower)
        if m:
            # If the number is reasonable for a field event (between 0.3 and 150), assume meters
            # Covers: PV (3-6m), HJ (1.5-2.5m), LJ (5-8m), TJ (12-18m), SP (10-20m), DT (40-60m), HT (50-80m), JT (50-80m)
            val = float(m.group(1))
            if 0.3 <= val <= 150:
                return ParsedPerf(value=val, unit="m", better_is="higher")

    # Check if it's a field mark format (has 'm' but not a distance like "300m" in event name)
    if "m" in s_lower and not re.search(r"\b\d+\s*m\b", s_lower) and not re.search(r"\b\d{2,4}m\b", event_lower):
        # field mark like "6.14m"
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*m", s_lower)
        if m:
            return ParsedPerf(value=float(m.group(1)), unit="m", better_is="higher")

    if ":" in s_lower:
        # time like "1:52.34" or "12:34"
        parts = [p.strip() for p in s_lower.split(":")]
        if len(parts) == 2:
            minutes = _first_number(parts[0])
            seconds = _first_number(parts[1])
            if minutes is None or seconds is None:
                return None
            return ParsedPerf(value=int(minutes) * 60 + float(seconds), unit="s", better_is="lower")
        if len(parts) == 3:
            hours = _first_number(parts[0])
            minutes = _first_number(parts[1])
            seconds = _first_number(parts[2])
            if hours is None or minutes is None or seconds is None:
                return None
            return ParsedPerf(value=int(hours) * 3600 + int(minutes) * 60 + float(seconds), unit="s", better_is="lower")

    # default numeric: assume time (seconds) for track & XC
    m = re.search(r"(-?\d+(?:\.\d+)?)", s_lower)
    if not m:
        return None
    return ParsedPerf(value=float(m.group(1)), unit="s", better_is="lower")


def within_last_n_seasons(season: str, years_back: int, now: Optional[dt.date] = None) -> bool:
    now = now or dt.date.today()
    years = _season_years(season)
    if not years:
        return False
    start_year, _end_year = years
    cutoff = now.year - max(1, int(years_back))
    return start_year >= cutoff


def compute_pbs(
    athlete_url: str,
    athlete_name: str,
    athlete_sex: Optional[str],
    rows: Iterable[PerformanceRow],
) -> list[dict]:
    best_by_event: dict[str, tuple[PerformanceRow, ParsedPerf]] = {}
    for r in rows:
        event = canonical_event_name(r.event)
        parsed = parse_perf(r.perf_raw, event)
        if not parsed:
            continue
        cur = best_by_event.get(event)
        if not cur:
            # Store a normalized event name for consistent output
            r_norm = dataclasses.replace(r, event=event)
            best_by_event[event] = (r_norm, parsed)
            continue
        _, cur_parsed = cur
        if parsed.better_is == "lower":
            if parsed.value < cur_parsed.value:
                r_norm = dataclasses.replace(r, event=event)
                best_by_event[event] = (r_norm, parsed)
        else:
            if parsed.value > cur_parsed.value:
                r_norm = dataclasses.replace(r, event=event)
                best_by_event[event] = (r_norm, parsed)

    out: list[dict] = []
    for event, (r, p) in sorted(best_by_event.items(), key=lambda kv: kv[0].lower()):
        out.append(
            {
                "university": "Brock University",
                "athlete_name": athlete_name,
                "sex": athlete_sex or "",
                "event": event,
                "pb_raw": r.perf_raw,
                "pb_value": f"{p.value:.4f}".rstrip("0").rstrip("."),
                "pb_unit": p.unit,
                "better_is": p.better_is,
                "pb_date": (r.date.isoformat() if r.date else ""),
                "pb_date_raw": r.date_raw,
                "pb_season": r.season,
                "pb_meet": r.meet,
                "pb_meet_url": r.meet_url or "",
                "athlete_url": athlete_url,
            }
        )
    return out


def compute_pbs_from_rankings_entries(
    athlete_url: str,
    athlete_name: str,
    athlete_sex: Optional[str],
    entries: Iterable[dict],
) -> list[dict]:
    """
    Compute PBs per event from rankings entries (no meet/date available).
    """
    best_by_event: dict[str, tuple[str, ParsedPerf]] = {}
    for e in entries:
        if e.get("athlete_url") != athlete_url:
            continue
        event = canonical_event_name(e.get("event") or "")
        perf_raw = e.get("perf_raw") or ""
        if not event or not perf_raw:
            continue
        parsed = parse_perf(perf_raw, event)
        if not parsed:
            # rankings sometimes omit units; infer direction based on event name
            direction = _infer_better_is_from_event_name(event)
            m = re.search(r"(-?\\d+(?:\\.\\d+)?)", perf_raw)
            if not m:
                continue
            val = float(m.group(1))
            parsed = ParsedPerf(value=val, unit=("m" if direction == "higher" else "s"), better_is=direction)

        cur = best_by_event.get(event)
        if not cur:
            best_by_event[event] = (perf_raw, parsed)
            continue
        _, cur_parsed = cur
        if parsed.better_is == "lower":
            if parsed.value < cur_parsed.value:
                best_by_event[event] = (perf_raw, parsed)
        else:
            if parsed.value > cur_parsed.value:
                best_by_event[event] = (perf_raw, parsed)

    out: list[dict] = []
    for event, (perf_raw, p) in sorted(best_by_event.items(), key=lambda kv: kv[0].lower()):
        out.append(
            {
                "university": "Brock University",
                "athlete_name": athlete_name,
                "sex": athlete_sex or "",
                "event": event,
                "pb_raw": perf_raw,
                "pb_value": f"{p.value:.4f}".rstrip("0").rstrip("."),
                "pb_unit": p.unit,
                "better_is": p.better_is,
                "pb_date": "",
                "pb_date_raw": "",
                "pb_season": "",
                "pb_meet": "",
                "pb_meet_url": "",
                "athlete_url": athlete_url,
            }
        )
    return out


def scrape_one_athlete(
    client: TrackieClient,
    athlete_url: str,
    years_back: int,
    sex_override: Optional[str] = None,
) -> list[dict]:
    html = client.get_html(athlete_url)
    soup = BeautifulSoup(html, "lxml")
    name = parse_athlete_name(soup) or athlete_url
    sex = sex_override or parse_athlete_sex(soup)
    rows = parse_performance_rows(html, athlete_url)
    rows = [r for r in rows if within_last_n_seasons(r.season, years_back)]
    return compute_pbs(athlete_url=athlete_url, athlete_name=name, athlete_sex=sex, rows=rows)


def _validate_url(url: str) -> str:
    p = urlparse(url)
    if p.scheme not in {"http", "https"}:
        raise argparse.ArgumentTypeError("URL must start with http:// or https://")
    return url


def _infer_university_id(university_url: str) -> Optional[int]:
    # e.g. https://www.trackie.com/usports/tnf/universities/brock-university/3/
    parts = [p for p in urlparse(university_url).path.split("/") if p]
    for part in reversed(parts):
        if part.isdigit():
            return int(part)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch PBs from Trackie athlete pages and export CSV.")
    ap.add_argument("--university-url", type=_validate_url, default=DEFAULT_UNIVERSITY_URL)
    ap.add_argument("--years-back", type=int, default=5, help="How many seasons back to include (default: 5).")
    ap.add_argument("--out", default="brock_pbs.csv", help="Output CSV path.")
    ap.add_argument(
        "--include-past-athletes",
        action="store_true",
        default=True,
        help="Also discover athletes from historical rankings pages (default: enabled).",
    )
    ap.add_argument(
        "--no-include-past-athletes",
        action="store_false",
        dest="include_past_athletes",
        help="Only use the current roster page athlete list (disable historical discovery).",
    )
    ap.add_argument(
        "--university-id",
        type=int,
        default=0,
        help="Trackie university numeric id (optional; inferred from --university-url if 0).",
    )
    ap.add_argument("--delay-seconds", type=float, default=0.6, help="Polite delay between requests.")
    ap.add_argument("--max-workers", type=int, default=6, help="Concurrent athlete fetchers.")
    ap.add_argument("--timeout-seconds", type=float, default=30.0)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--max-athletes", type=int, default=0, help="Debug: limit number of athletes (0 = no limit).")
    args = ap.parse_args()

    client = TrackieClient(
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
    )

    uni_html = client.get_html(args.university_url)
    athlete_index = parse_university_athlete_index(uni_html, args.university_url)

    uni_id = int(args.university_id) if int(args.university_id) > 0 else (_infer_university_id(args.university_url) or 0)
    rankings_entries_by_athlete: dict[str, list[dict]] = {}

    # Add historical athletes from rankings pages (covers past athletes not present in current roster).
    if args.include_past_athletes:
        if not uni_id:
            print("[warn] Could not infer university id; skipping past athlete discovery.")
        else:
            for end_year in _season_end_years(int(args.years_back)):
                rankings_url = f"https://www.trackie.com/usports/tnf/rankings/universities/{uni_id}/{end_year}/"
                try:
                    rhtml = client.get_html(rankings_url)
                    for ent in parse_rankings_entries(rhtml, rankings_url):
                        rankings_entries_by_athlete.setdefault(ent["athlete_url"], []).append(ent)
                    r_index = parse_rankings_athlete_index(rhtml, rankings_url)
                    # Merge (keep roster sex if present, otherwise use inferred)
                    for url, meta in r_index.items():
                        if url not in athlete_index:
                            athlete_index[url] = meta
                        else:
                            if not athlete_index[url].get("sex") and meta.get("sex"):
                                athlete_index[url]["sex"] = meta["sex"]
                            if not athlete_index[url].get("name") and meta.get("name"):
                                athlete_index[url]["name"] = meta["name"]
                except Exception as e:
                    print(f"[warn] Failed rankings discovery for {rankings_url}: {e}")

    athlete_urls = sorted(athlete_index.keys())
    if args.max_athletes and args.max_athletes > 0:
        athlete_urls = athlete_urls[: args.max_athletes]

    if not athlete_urls:
        raise RuntimeError("No athlete URLs found on the university page. Trackie page structure may have changed.")

    all_rows: list[dict] = []

    # Use a pool, but keep global rate limiting so we don't hammer Trackie.
    with ThreadPoolExecutor(max_workers=max(1, int(args.max_workers))) as ex:
        futures = {
            ex.submit(
                scrape_one_athlete,
                client,
                url,
                int(args.years_back),
                athlete_index.get(url, {}).get("sex") or None,
            ): url
            for url in athlete_urls
        }
        pbar = tqdm(total=len(futures), desc="Athletes", unit="athlete") if tqdm is not None else None
        try:
            for fut in as_completed(futures):
                url = futures[fut]
                try:
                    rows = fut.result()
                    if not rows:
                        # Fallback: if the athlete profile page is empty/missing performance tables,
                        # compute PBs from rankings entries (season-best) across the requested window.
                        fallback_entries = rankings_entries_by_athlete.get(url, [])
                        if fallback_entries:
                            meta = athlete_index.get(url, {})
                            name = meta.get("name") or url
                            sex = meta.get("sex") or ""
                            rows = compute_pbs_from_rankings_entries(url, name, sex, fallback_entries)
                    all_rows.extend(rows)
                except Exception as e:
                    print(f"[warn] Failed athlete {url}: {e}")
                if pbar is not None:
                    pbar.update(1)
        finally:
            if pbar is not None:
                pbar.close()

    fieldnames = [
        "university",
        "athlete_name",
        "sex",
        "event",
        "pb_raw",
        "pb_value",
        "pb_unit",
        "better_is",
        "pb_date",
        "pb_date_raw",
        "pb_season",
        "pb_meet",
        "pb_meet_url",
        "athlete_url",
    ]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in all_rows:
            w.writerow(r)

    print(f"Wrote {len(all_rows)} PB rows to {args.out} (from {len(athlete_urls)} athletes).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


