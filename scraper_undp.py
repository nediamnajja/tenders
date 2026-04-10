"""
scraper_undp.py — UNDP Procurement Notices Historical Scraper
=============================================================
Fetches all currently active tenders from UNDP portal.
Run once to populate the database with all open tenders.

Pipeline position:
    THIS FILE → Save raw data → enricher_undp.py → normalizer.py

How it works:
    - Fetches month by month going backwards from current month
    - If a month returns exactly 1000 rows → splits into weeks
    - Filters expired tenders on our side (deadline < today)
    - Stops automatically when 2 consecutive months return zero active tenders
    - Saves all active tenders to the database

Usage:
    python scraper_undp.py              (full backfill)
    python scraper_undp.py --preview    (first month only, no save)
"""

import argparse
import logging
import re
import time
from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from db import init_db, get_session, upsert_organisation, upsert_tender
from models import ScraperState

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────

BASE_URL      = "https://procurement-notices.undp.org"
SEARCH_URL    = f"{BASE_URL}/search.cfm"
PORTAL        = "undp"
ROW_LIMIT     = 1000   # UNDP portal max rows per request
REQUEST_DELAY = 2      # seconds between requests
TODAY         = date.today().isoformat()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": SEARCH_URL,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────────────────────────

def save_state(portal: str, last_notice_id: str) -> None:
    """Save newest notice ID after backfill to seed the daily scraper."""
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=portal).first()
        if state:
            state.last_run       = TODAY
            state.last_notice_id = last_notice_id
        else:
            state = ScraperState(portal=portal, last_run=TODAY, last_notice_id=last_notice_id)
            session.add(state)
    logger.info(f"Cursor seeded — last_notice_id: {last_notice_id}")


def get_newest_id_from_db() -> Optional[str]:
    """Get the newest UNDP tender ID from the database."""
    with get_session() as session:
        row = session.execute(text("""
            SELECT tender_id FROM tenders
            WHERE source_portal = 'undp'
            ORDER BY publication_date DESC, id DESC
            LIMIT 1
        """)).fetchone()
        return row[0] if row else None


# ─────────────────────────────────────────────────────────────
#  DATE RANGE HELPERS
# ─────────────────────────────────────────────────────────────

def month_ranges_going_back(start_year: int, start_month: int):
    """
    Generator — yields (date_from, date_to) tuples going backwards
    month by month from start_year/start_month.
    """
    year, month = start_year, start_month
    while True:
        last_day  = monthrange(year, month)[1]
        date_from = f"{year}-{month:02d}-01"
        date_to   = f"{year}-{month:02d}-{last_day:02d}"
        yield date_from, date_to

        month -= 1
        if month == 0:
            month = 12
            year -= 1


def week_ranges(date_from: str, date_to: str):
    """
    Split a date range into weekly chunks.
    Used when a month hits the 1000 row limit.
    """
    start   = datetime.strptime(date_from, "%Y-%m-%d").date()
    end     = datetime.strptime(date_to,   "%Y-%m-%d").date()
    current = start

    while current <= end:
        week_end = min(current + timedelta(days=6), end)
        yield current.isoformat(), week_end.isoformat()
        current = week_end + timedelta(days=1)


# ─────────────────────────────────────────────────────────────
#  STEP 1 — FETCH
# ─────────────────────────────────────────────────────────────

def fetch(date_from: str, date_to: str) -> Optional[str]:
    """
    POST search form for tenders published between date_from and date_to.
    Returns raw HTML or None if request failed.
    """
    payload = {
        "date_from1":    date_from,
        "date_to1":      date_to,
        "cur_sm_id":     "",
        "cur_title":     "",
        "cur_notice_id": "",
        "cur_agency":    "",
    }

    try:
        logger.info(f"  Fetching {date_from} → {date_to}")
        response = requests.post(
            SEARCH_URL,
            data=payload,
            headers=HEADERS,
            timeout=60,
        )
        response.raise_for_status()
        return response.text

    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to UNDP portal")
        return None
    except requests.exceptions.Timeout:
        logger.error("Request timed out")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e}")
        return None


# ─────────────────────────────────────────────────────────────
#  STEP 2 — PARSE
# ─────────────────────────────────────────────────────────────

def _get_cell(row, label: str) -> str:
    for cell in row.find_all("div", class_="vacanciesTable__cell"):
        label_div = cell.find("div", class_="vacanciesTable__cell__label")
        if label_div and label_div.get_text(strip=True).lower() == label.lower():
            span = cell.find("span")
            if span:
                return span.get_text(separator=" ", strip=True)
    return ""


def _parse_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    date_part = raw.split()[0].strip()
    for fmt in ["%d-%b-%y", "%d-%b-%Y"]:
        try:
            return datetime.strptime(date_part, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_time(raw: str) -> Optional[str]:
    match = re.search(r"(\d{1,2}:\d{2}\s*(?:AM|PM))", raw, re.IGNORECASE)
    return match.group(1).strip() if match else None


def _parse_office(raw: str) -> tuple[str, str]:
    if "/" in raw:
        parts = raw.split("/", 1)
        return parts[0].strip(), parts[1].strip().title()
    return raw.strip(), ""


def parse_html(html: str) -> tuple[list[dict], int]:
    """
    Parse all tender rows from HTML.
    Returns (active_tenders, total_row_count).
    total_row_count includes expired — used to detect 1000 row limit.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("a", class_="vacanciesTableLink")

    if not rows:
        return [], 0

    total_rows = len(rows)
    results    = []
    expired    = 0
    no_date    = 0

    for row in rows:
        try:
            href  = row.get("href", "")
            match = re.search(r"nego_id=(\d+)", href)
            if not match:
                continue
            nego_id = match.group(1)

            deadline_raw = _get_cell(row, "Deadline")
            deadline     = _parse_date(deadline_raw)

            if not deadline:
                no_date += 1
                continue

            if deadline < TODAY:
                expired += 1
                continue

            org_raw, country = _parse_office(_get_cell(row, "UNDP Office/Country"))

            results.append({
                "nego_id":       nego_id,
                "title":         _get_cell(row, "Title"),
                "ref_no":        _get_cell(row, "Ref No"),
                "org_name":      org_raw or "UNDP",
                "country":       country or None,
                "process":       _get_cell(row, "Process"),
                "deadline":      deadline,
                "deadline_time": _parse_time(deadline_raw),
                "posted":        _parse_date(_get_cell(row, "Posted")),
                "source_url":    f"{BASE_URL}/{href}",
            })

        except Exception as e:
            logger.warning(f"Row parse error: {e}")
            continue

    logger.info(
        f"  Rows: {total_rows} total | "
        f"{len(results)} active | "
        f"{expired} expired | "
        f"{no_date} no date"
    )
    return results, total_rows


# ─────────────────────────────────────────────────────────────
#  STEP 3 — SAVE
# ─────────────────────────────────────────────────────────────

def _save(row: dict) -> dict:
    org_id = upsert_organisation(
        name=row["org_name"],
        organisation_type="International",
        country=row["country"],
    )

    return upsert_tender({
        "source_portal":           PORTAL,
        "tender_id":               row["nego_id"],
        "title":                   row["title"],
        "description":             None,
        "country":                 row["country"],
        "notice_type":             row["process"],
        "language":                "English",
        "publication_date":        row["posted"],
        "deadline_date":           row["deadline"],
        "deadline_time":           row["deadline_time"],
        "budget":                  None,
        "currency":                None,
        "project_id":              row["ref_no"],
        "procurement_group":       None,
        "procurement_method_code": None,
        "procurement_method_name": row["process"],
        "sector":                  None,
        "keywords":                None,
        "summary":                 None,
        "status_id":               4,
        "organisation_id":         org_id,
        "source_url":              row["source_url"],
    })


def save_batch(tenders: list[dict], preview: bool) -> tuple[int, int]:
    """Save a batch of active tenders. Returns (saved, errors)."""
    saved  = 0
    errors = 0

    for row in tenders:
        if preview:
            print(f"    nego_id  : {row['nego_id']}")
            print(f"    title    : {row['title'][:60]}")
            print(f"    country  : {row['country']}")
            print(f"    deadline : {row['deadline']}")
            print()
            saved += 1
            continue

        try:
            result = _save(row)
            logger.info(
                f"  ✅  {result['title'][:55]:<55} "
                f"| {result['country'] or 'N/A':<20} "
                f"| {result['deadline_date']}"
            )
            saved += 1
        except Exception as e:
            logger.error(f"  ❌  Save failed: {e} | nego_id={row['nego_id']}")
            errors += 1

    return saved, errors


# ─────────────────────────────────────────────────────────────
#  FETCH AND PROCESS ONE DATE RANGE
#  Handles week splitting if month hits 1000 row limit
# ─────────────────────────────────────────────────────────────

def process_range(date_from: str, date_to: str, preview: bool) -> tuple[int, int, int]:
    """
    Fetch and process one date range.
    If result hits 1000 row limit → split into weeks automatically.
    Returns (saved, errors, active_count).
    """
    html = fetch(date_from, date_to)
    if not html:
        return 0, 0, 0

    time.sleep(REQUEST_DELAY)
    tenders, total_rows = parse_html(html)

    # Hit the row limit — split into weeks
    if total_rows >= ROW_LIMIT:
        logger.warning(
            f"  ⚠️  Hit {ROW_LIMIT} row limit — "
            f"splitting {date_from} → {date_to} into weeks"
        )
        total_saved  = 0
        total_errors = 0
        total_active = 0

        for week_from, week_to in week_ranges(date_from, date_to):
            logger.info(f"  Week: {week_from} → {week_to}")
            week_html = fetch(week_from, week_to)
            if not week_html:
                continue
            time.sleep(REQUEST_DELAY)
            week_tenders, _ = parse_html(week_html)
            s, e = save_batch(week_tenders, preview)
            total_saved  += s
            total_errors += e
            total_active += len(week_tenders)

        return total_saved, total_errors, total_active

    # Normal — save directly
    saved, errors = save_batch(tenders, preview)
    return saved, errors, len(tenders)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run(preview: bool = False) -> None:

    today = date.today()
    year  = today.year
    month = today.month

    logger.info("=" * 65)
    logger.info("UNDP Historical Scraper — Stage 1 Ingestion")
    logger.info(f"Today    : {TODAY}")
    logger.info(f"Starting : {year}-{month:02d}")
    logger.info(f"Mode     : {'PREVIEW — no saves' if preview else 'FULL BACKFILL'}")
    logger.info("=" * 65)

    total_saved       = 0
    total_errors      = 0
    months_processed  = 0
    consecutive_empty = 0

    for date_from, date_to in month_ranges_going_back(year, month):
        months_processed += 1
        logger.info(f"\n{'─' * 65}")
        logger.info(f"Month {months_processed}: {date_from} → {date_to}")
        logger.info(f"{'─' * 65}")

        saved, errors, active = process_range(date_from, date_to, preview)

        total_saved  += saved
        total_errors += errors

        if active == 0:
            consecutive_empty += 1
            logger.info(
                f"  No active tenders — "
                f"consecutive empty months: {consecutive_empty}/2"
            )
        else:
            consecutive_empty = 0
            logger.info(f"  Month total — saved:{saved} errors:{errors}")

        if consecutive_empty >= 2:
            logger.info(
                "\n2 consecutive months with no active tenders — "
                "stopping, nothing older will be active"
            )
            break

        if preview:
            logger.info("Preview mode — stopping after first month")
            break

    if not preview:
        newest_id = get_newest_id_from_db()
        if newest_id:
            save_state(PORTAL, newest_id)
            logger.info(f"Daily scraper cursor seeded: {newest_id}")

    logger.info("\n" + "=" * 65)
    logger.info("BACKFILL COMPLETE")
    logger.info(f"Months processed : {months_processed}")
    logger.info(f"Total saved      : {total_saved}")
    logger.info(f"Total errors     : {total_errors}")
    logger.info("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UNDP Historical Scraper")
    parser.add_argument("--preview", action="store_true", help="First month only, no saves")
    args = parser.parse_args()
    init_db()
    run(preview=args.preview)