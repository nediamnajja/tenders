
"""
scraper_worldbank_daily.py — World Bank Daily Scraper
======================================================
Stage 1 — Ingestion layer. Runs every day via scheduler (e.g. cron at 08:00).

Pipeline position:
    THIS FILE → Save raw notice_text → enricher_worldbank.py → normalizer.py

Filters applied:
    - publication_date must be in 2026
    - deadline must not have already passed
    - deadline year is NOT checked (2026 tenders may close in 2027+)

Fixes vs previous version:
    - notice_url  built from notice id  (was storing contact website by mistake)
    - contact_web_url stored separately as borrower/org website
    - FIX #1  cursor safety: finishes full page after cursor hit, stops after page
    - FIX #2  empty DB abort: hard-stops with clear message if no cursor and DB empty
    - FIX #3  scraped_at exposed via created_at in saved dict
    - FIX #6  date comparisons use date objects, not strings
    - Run stats persisted to scraper_run_log for dashboard

Usage:
    python scraper_worldbank_daily.py           (normal daily run)
    python scraper_worldbank_daily.py --reset   (clear cursor, re-fetch from scratch)
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timezone
from typing import Optional

import requests

from db import (
    init_db, get_state, save_state, reset_state,
    get_newest_id_from_db, upsert_organisation, upsert_tender,
    add_contact, get_tender_by_ref, log_scraper_run,
)

API_URL             = "https://search.worldbank.org/api/v2/procnotices"
NOTICE_URL_TEMPLATE = "https://projects.worldbank.org/en/projects-operations/procurement-detail/{}"
PORTAL              = "worldbank"
PAGE_SIZE           = 500
REQUEST_DELAY       = 2
MAX_RETRIES         = 3
TODAY               = date.today()        # FIX #6 — date object
TODAY_ISO           = TODAY.isoformat()
YEAR_FILTER         = "2026"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SIGNAL_CONTINUE = "continue"
SIGNAL_STOP     = "stop"      # cursor reached — stop after finishing current page
SIGNAL_EXPIRED  = "expired"   # full page disqualified — stop


# ─────────────────────────────────────────────────────────────
#  FETCH
# ─────────────────────────────────────────────────────────────

def fetch_page(offset: int, retries: int = 0) -> Optional[dict]:
    params = {"format": "json", "rows": PAGE_SIZE, "os": offset}
    try:
        logger.info(f"Fetching offset={offset}...")
        r = requests.get(API_URL, params=params, timeout=30)

        if r.status_code == 429:
            if retries < MAX_RETRIES:
                logger.warning("Rate limited — waiting 60s")
                time.sleep(60)
                return fetch_page(offset, retries + 1)
            return None

        if r.status_code in (500, 502, 503, 504):
            if retries < MAX_RETRIES:
                wait = 5 * (2 ** retries)
                logger.warning(f"Server error {r.status_code} — retrying in {wait}s")
                time.sleep(wait)
                return fetch_page(offset, retries + 1)
            return None

        r.raise_for_status()
        data = r.json()
        logger.info(f"Received {len(data.get('procnotices', []))} records (total:{data.get('total','?')})")
        return data

    except requests.exceptions.ConnectionError:
        if retries < MAX_RETRIES:
            wait = 5 * (2 ** retries)
            logger.warning(f"Connection error — retrying in {wait}s")
            time.sleep(wait)
            return fetch_page(offset, retries + 1)
        logger.error("Cannot connect to World Bank API")
        return None

    except requests.exceptions.Timeout:
        if retries < MAX_RETRIES:
            wait = 5 * (2 ** retries)
            logger.warning(f"Timeout — retrying in {wait}s")
            time.sleep(wait)
            return fetch_page(offset, retries + 1)
        logger.error("Timeout — max retries exceeded")
        return None

    except (requests.exceptions.HTTPError, ValueError) as e:
        logger.error(f"Error: {e}")
        return None


# ─────────────────────────────────────────────────────────────
#  PARSE
# ─────────────────────────────────────────────────────────────

def _parse_date(raw: str) -> Optional[date]:
    """FIX #6 — returns date object so all comparisons are between dates."""
    if not raw:
        return None
    if "T" in raw:
        raw = raw.split("T")[0]
    for fmt in ["%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y"]:
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse_notice(notice: dict) -> Optional[dict]: 
    """
    Read one API record. Returns None only on hard failures (missing id/title/deadline).
    Filtering by year/expiry happens in process_page so the cursor
    check can see every record regardless.
    """
    notice_id = notice.get("id", "").strip()
    if not notice_id:
        return None

    title = notice.get("project_name", "")
    if not title:
        return None

    deadline = _parse_date(notice.get("submission_deadline_date", ""))
    if not deadline:
        return None

    pub_date = _parse_date(notice.get("noticedate", ""))

    return {
        "notice_id":               notice_id,
        "title":                   title,
        "notice_text":             notice.get("notice_text") or None,
        "description":             notice.get("bid_description") or None,
        "country":                 notice.get("project_ctry_name") or None,
        "publication_date":        pub_date.isoformat() if pub_date else None,
        "deadline_date":           deadline.isoformat(),
        "deadline_time":           notice.get("submission_deadline_time") or None,
        "notice_type":             notice.get("notice_type") or None,
        "language":                notice.get("notice_lang_name") or "English",
        "project_id":              notice.get("project_id") or None,
        "procurement_group":       notice.get("procurement_group") or None,
        "procurement_method_code": notice.get("procurement_method_code") or None,
        "procurement_method_name": notice.get("procurement_method_name") or None,
        "org_name":                notice.get("contact_organization") or "World Bank",
        "org_country":             notice.get("contact_ctry_name") or None,
        "contact_name":            notice.get("contact_name") or None,
        "contact_email":           notice.get("contact_email") or None,
        "contact_phone":           notice.get("contact_phone_no") or None,
        "contact_address":         notice.get("contact_address") or None,
        # URL FIX — notice_url = direct link to this specific notice page
        # contact_web_url = borrower/org website (completely different field)
        "notice_url":              NOTICE_URL_TEMPLATE.format(notice_id),
        "contact_web_url":         notice.get("contact_web_url") or None,
        # internal — date objects for filtering only, not stored in DB
        "_deadline_obj":           deadline,
        "_pub_date_obj":           pub_date,
        "notice_text":             notice.get("notice_text") or None,  # raw — enricher reads this
    }


# ─────────────────────────────────────────────────────────────
#  PROCESS PAGE  (cursor logic + filters)
# ─────────────────────────────────────────────────────────────

def process_page(notices: list[dict], last_notice_id: Optional[str], stats: dict) -> str:
    """
    Apply cursor logic and ingestion filters to one page.

    FIX #1 — cursor safety:
        When last_notice_id is found, we set a flag and keep processing
        the rest of the page instead of returning immediately.
        SIGNAL_STOP is returned only after the full page is done.

    Per-record order:
        1. Cursor check  — set stop flag, skip this record (already in DB)
        2. Dedup         — skip if already in DB
        3. Expired       — FIX #6: compare date objects
        4. Year filter   — publication_date must be 2026
        5. Save
    """
    cursor_hit       = False
    all_disqualified = True

    for notice in notices:
        try:
            row = parse_notice(notice)
            if row is None:
                continue

            notice_id = row["notice_id"]

            # 1. FIX #1 — cursor: mark hit, finish the page, stop after
            if last_notice_id and notice_id == last_notice_id:
                logger.info(f"  ⏹  Cursor hit (id={notice_id}) — finishing page then stopping")
                stats["skipped_cursor"] += 1
                cursor_hit = True
                continue   # ← critical: was "return SIGNAL_STOP" before

            # 2. Safety dedup
            if get_tender_by_ref(PORTAL, notice_id):
                logger.debug(f"  ⏭  Already exists — {notice_id}")
                stats["skipped_existing"] += 1
                all_disqualified = False
                continue

            # 3. FIX #6 — compare date objects, not strings
            if row["_deadline_obj"] < TODAY:
                stats["expired"] += 1
                continue

            # 4. Year filter — publication_date must be 2026
            pub_obj = row["_pub_date_obj"]
            if not pub_obj or str(pub_obj.year) != YEAR_FILTER:
                stats["filtered_year"] += 1
                continue

            # Qualifying tender — save it
            all_disqualified = False
            saved = _save(row)

            if stats["newest_notice_id"] is None:
                stats["newest_notice_id"] = notice_id

            logger.info(
                f"  ✅  {saved['title'][:50]:<50} "
                f"| {saved['country'] or 'N/A':<20} "
                f"| deadline:{saved['deadline_date']} "
                f"| scraped_at:{saved.get('scraped_at', 'N/A')}"   # FIX #3
            )
            stats["saved"] += 1

        except Exception as e:
            logger.error(f"  ❌  {e} | id={notice.get('id', '?')}")
            stats["errors"] += 1

    # FIX #1 — stop only after full page is processed
    if cursor_hit:
        return SIGNAL_STOP

    if all_disqualified and not last_notice_id:
        return SIGNAL_EXPIRED

    return SIGNAL_CONTINUE


# ─────────────────────────────────────────────────────────────
#  SAVE
# ─────────────────────────────────────────────────────────────

def _save(row: dict) -> dict:
    org_id = upsert_organisation(
        name=row["org_name"],
        organisation_type="International",
        country=row["org_country"],
    )

    if row.get("contact_name") or row.get("contact_email"):
        add_contact(
            organisation_id=org_id,
            name=row["contact_name"],
            email=row["contact_email"],
            phone=row["contact_phone"],
            address=row["contact_address"],
        )

    return upsert_tender({
        "source_portal":           PORTAL,
        "tender_id":               row["notice_id"],
        "title":                   row["title"],
        "notice_text":             row["notice_text"],
        "description":             row["description"],
        "country":                 row["country"],
        "notice_type":             row["notice_type"],
        "language":                row["language"],
        "publication_date":        row["publication_date"],
        "deadline_date":           row["deadline_date"],
        "deadline_time":           row["deadline_time"],
        "budget":                  None,
        "currency":                None,
        "project_id":              row["project_id"],
        "procurement_group":       row["procurement_group"],
        "procurement_method_code": row["procurement_method_code"],
        "procurement_method_name": row["procurement_method_name"],
        "sector":                  None,
        "keywords":                None,
        "summary":                 None,
        "status_id":               4,
        "organisation_id":         org_id,
        # URL FIX — direct notice page link, not the borrower website
        "source_url":              row["notice_url"],
    })


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run(state: dict) -> None:
    started_at     = datetime.now(timezone.utc)
    last_notice_id = state["last_notice_id"]

    # ── FIX #2 — Bootstrap or hard-abort ─────────────────────
    if not last_notice_id:
        db_newest = get_newest_id_from_db(PORTAL)
        if db_newest:
            logger.info(f"No cursor in state — bootstrapping from DB: {db_newest}")
            last_notice_id = db_newest
            save_state(PORTAL, last_notice_id)
        else:
            msg = (
                "ABORT: DB is empty and no cursor exists. "
                "Run scraper_worldbank.py (historical backfill) first, "
                "then re-run this daily scraper."
            )
            logger.error(msg)
            log_scraper_run(
                portal     = PORTAL,
                started_at = started_at,
                status     = "empty_db_abort",
                notes      = msg,
            )
            sys.exit(1)
    # ─────────────────────────────────────────────────────────

    logger.info("=" * 65)
    logger.info("World Bank Daily Scraper — Stage 1 Ingestion")
    logger.info(f"Today       : {TODAY_ISO}")
    logger.info(f"Year filter : publication_date in {YEAR_FILTER}")
    logger.info(f"Cursor      : {last_notice_id}")
    logger.info("=" * 65)

    stats = {
        "saved":            0,
        "skipped_cursor":   0,
        "skipped_existing": 0,
        "expired":          0,
        "filtered_year":    0,
        "errors":           0,
        "newest_notice_id": None,
    }

    offset   = 0
    page_num = 0

    try:
        while True:
            page_num += 1
            logger.info(f"\n--- Page {page_num} (offset={offset}) ---")

            data = fetch_page(offset)
            if not data:
                logger.error("Fetch failed — stopping")
                break

            notices = data.get("procnotices", [])
            if not notices:
                logger.info("No more records from API")
                break

            signal = process_page(notices, last_notice_id, stats)

            if signal == SIGNAL_STOP:
                logger.info("Cursor page complete — done")
                break
            if signal == SIGNAL_EXPIRED:
                logger.info("Full page disqualified — done")
                break

            offset += PAGE_SIZE
            time.sleep(REQUEST_DELAY)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        stats["errors"] += 1

    # ── Determine run status ──────────────────────────────────
    if stats["errors"] > 0 and stats["saved"] == 0:
        run_status = "failed"
    elif stats["errors"] > 0:
        run_status = "partial"
    else:
        run_status = "success"

    # ── Save cursor ───────────────────────────────────────────
    new_cursor = stats["newest_notice_id"] or last_notice_id
    if run_status != "failed":
        save_state(PORTAL, new_cursor)
    else:
        logger.warning("Run failed — cursor not updated")

    # ── Persist run stats for dashboard ──────────────────────
    log_scraper_run(
        portal          = PORTAL,
        started_at      = started_at,
        status          = run_status,
        saved           = stats["saved"],
        expired_skipped = stats["expired"],
        year_filtered   = stats["filtered_year"],
        cursor_stopped  = stats["skipped_cursor"],
        already_existed = stats["skipped_existing"],
        errors          = stats["errors"],
        new_cursor      = new_cursor,
    )

    logger.info("\n" + "=" * 65)
    logger.info(f"Status          : {run_status}")
    logger.info(f"Saved           : {stats['saved']}")
    logger.info(f"Cursor stopped  : {stats['skipped_cursor']}")
    logger.info(f"Already existed : {stats['skipped_existing']}")
    logger.info(f"Expired skipped : {stats['expired']}")
    logger.info(f"Pre-2026 skipped: {stats['filtered_year']}")
    logger.info(f"Errors          : {stats['errors']}")
    logger.info(f"New cursor      : {new_cursor}")
    logger.info("=" * 65)
    if stats["saved"] > 0:
        logger.info("Next step: python enricher_worldbank.py")
    logger.info("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="World Bank Daily Scraper")
    parser.add_argument("--reset", action="store_true", help="Clear cursor")
    args = parser.parse_args()
    init_db()
    state = reset_state(PORTAL) if args.reset else get_state(PORTAL)
    run(state)