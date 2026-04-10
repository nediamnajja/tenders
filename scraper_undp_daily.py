"""
scraper_undp_daily.py — UNDP Daily Scraper
===========================================
Runs every day via scheduler.
Uses dual cursor for precise incremental tracking:
    1. last_notice_id  → primary cursor, stops when we hit a known tender
    2. last_run_date   → safety net, catches anything missed if scraper was offline

Pipeline position:
    THIS FILE → Save raw data → enricher_undp.py → normalizer.py

Usage:
    python scraper_undp_daily.py              (normal daily run)
    python scraper_undp_daily.py --reset      (reset cursors, re-fetch last 7 days)
"""

import argparse
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

from db import init_db, get_session, upsert_organisation, upsert_tender, get_tender_by_ref
from models import ScraperState

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────

BASE_URL      = "https://procurement-notices.undp.org"
SEARCH_URL    = f"{BASE_URL}/search.cfm"
PORTAL        = "undp"
REQUEST_DELAY = 2
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

def get_state(portal: str) -> dict:
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=portal).first()
        if state:
            return {"last_run": state.last_run, "last_notice_id": state.last_notice_id}
        return {"last_run": None, "last_notice_id": None}


def save_state(portal: str, last_run: str, last_notice_id: str) -> None:
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=portal).first()
        if state:
            state.last_run       = last_run
            state.last_notice_id = last_notice_id
        else:
            state = ScraperState(portal=portal, last_run=last_run, last_notice_id=last_notice_id)
            session.add(state)
    logger.info(f"State saved — last_run:{last_run} last_notice_id:{last_notice_id}")


def reset_state(portal: str, days_back: int = 7) -> dict:
    reset_date = (date.today() - timedelta(days=days_back)).isoformat()
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=portal).first()
        if state:
            state.last_run       = reset_date
            state.last_notice_id = None
        else:
            session.add(ScraperState(portal=portal, last_run=reset_date))
    logger.info(f"State reset — last_run:{reset_date} last_notice_id:cleared")
    return {"last_run": reset_date, "last_notice_id": None}


# ─────────────────────────────────────────────────────────────
#  STEP 1 — FETCH
# ─────────────────────────────────────────────────────────────

def fetch(date_from: str, date_to: str) -> Optional[str]:
    payload = {
        "date_from1":    date_from,
        "date_to1":      date_to,
        "cur_sm_id":     "",
        "cur_title":     "",
        "cur_notice_id": "",
        "cur_agency":    "",
    }

    try:
        logger.info(f"Fetching {date_from} → {date_to}")
        response = requests.post(SEARCH_URL, data=payload, headers=HEADERS, timeout=60)
        response.raise_for_status()
        logger.info(f"Received {len(response.text):,} characters")
        return response.text

    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to UNDP portal")
        return None
    except requests.exceptions.Timeout:
        logger.error("Request timed out — will retry next run")
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


def parse_rows(html: str) -> list[dict]:
    """
    Parse all rows from HTML.
    Expired deadlines filtered out here. Cursor logic handled in process_rows().
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("a", class_="vacanciesTableLink")

    if not rows:
        logger.warning("No rows found — page structure may have changed")
        return []

    logger.info(f"Found {len(rows)} rows in HTML")

    results = []
    expired = 0
    no_date = 0

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

    logger.info(f"Active:{len(results)} expired:{expired} no_date:{no_date}")
    return results


# ─────────────────────────────────────────────────────────────
#  STEP 3 — PROCESS  (cursor logic)
# ─────────────────────────────────────────────────────────────

def process_rows(rows: list[dict], last_notice_id: Optional[str]) -> dict:
    """
    Process rows using dual cursor logic. Rows arrive newest first.
    Stops when last_notice_id is hit. Skips already-saved records.
    """
    stats = {
        "saved":            0,
        "skipped_cursor":   0,
        "skipped_existing": 0,
        "errors":           0,
        "newest_notice_id": None,
    }

    for i, row in enumerate(rows, 1):
        nego_id = row["nego_id"]

        # Cursor check — stop when we reach last known tender
        if last_notice_id and nego_id == last_notice_id:
            logger.info(f"  [{i:04}] ⏹  Cursor reached (nego_id={nego_id}) — stopping")
            stats["skipped_cursor"] = len(rows) - i + 1
            break

        # Safety dedup
        if get_tender_by_ref(PORTAL, nego_id):
            logger.debug(f"  [{i:04}] ⏭  Already exists — {nego_id}")
            stats["skipped_existing"] += 1
            continue

        # Save
        try:
            saved = _save(row)

            if stats["newest_notice_id"] is None:
                stats["newest_notice_id"] = nego_id

            logger.info(
                f"  [{i:04}] ✅  {saved['title'][:55]:<55} "
                f"| {saved['country'] or 'N/A':<20} "
                f"| {saved['deadline_date']}"
            )
            stats["saved"] += 1

        except Exception as e:
            logger.error(f"  [{i:04}] ❌  {e} | nego_id={nego_id}")
            stats["errors"] += 1

        if i % 50 == 0:
            time.sleep(REQUEST_DELAY)

    return stats


# ─────────────────────────────────────────────────────────────
#  SAVE
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


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run(state: dict) -> None:

    last_run       = state["last_run"]
    last_notice_id = state["last_notice_id"]

    date_from = last_run if last_run else (date.today() - timedelta(days=1)).isoformat()
    date_to   = TODAY

    logger.info("=" * 65)
    logger.info("UNDP Daily Scraper — Stage 1 Ingestion")
    logger.info(f"Fetch range : {date_from} → {date_to}")
    logger.info(f"Cursor      : {last_notice_id or 'none — first run'}")
    logger.info("=" * 65)

    html = fetch(date_from, date_to)
    if not html:
        logger.error("Fetch failed — cursors not updated, will retry next run")
        return

    rows = parse_rows(html)
    if not rows:
        logger.info("No new active tenders found")
        save_state(PORTAL, TODAY, last_notice_id)
        return

    stats = process_rows(rows, last_notice_id)

    if stats["errors"] == 0 or stats["saved"] > 0:
        new_notice_id = stats["newest_notice_id"] or last_notice_id
        save_state(PORTAL, TODAY, new_notice_id)
    else:
        logger.warning("Errors and nothing saved — cursor not updated")

    logger.info("\n" + "=" * 65)
    logger.info(f"Saved           : {stats['saved']}")
    logger.info(f"Cursor stop     : {stats['skipped_cursor']}")
    logger.info(f"Already existed : {stats['skipped_existing']}")
    logger.info(f"Errors          : {stats['errors']}")
    logger.info(f"New cursor      : {stats['newest_notice_id'] or last_notice_id}")
    logger.info("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UNDP Daily Scraper")
    parser.add_argument("--reset", action="store_true", help="Reset cursors — re-fetches last 7 days")
    args = parser.parse_args()
    init_db()
    state = reset_state(PORTAL, days_back=7) if args.reset else get_state(PORTAL)
    run(state)