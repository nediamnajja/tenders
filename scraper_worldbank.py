
"""
scraper_worldbank.py — World Bank Procurement Notices Historical Scraper
=========================================================================
Stage 1 — Ingestion layer. Run ONCE for full backfill.

Pipeline position:
    THIS FILE → Save raw notice_text → enricher_worldbank.py → normalizer.py

Filters applied:
    - publication_date must be in 2026
    - deadline must not have already passed
    - deadline year is NOT checked (2026 tenders may close in 2027+)

Fixes vs previous version:
    - notice_url  built from notice id  (was storing contact website by mistake)
    - contact_web_url stored separately as borrower/org website
    - FIX #6  date comparisons use date objects, not strings
    - FIX #3  scraped_at exposed via created_at in saved dict
    - Run stats persisted to scraper_run_log for dashboard

Usage:
    python scraper_worldbank.py              (full backfill)
    python scraper_worldbank.py --preview    (first page only, no saves)
"""

import argparse
import logging
import time
from datetime import date, datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

from db import (
    init_db, get_session, upsert_organisation, upsert_tender,
    add_contact, get_newest_id_from_db, save_state, log_scraper_run,
)
from models import ScraperState

API_URL              = "https://search.worldbank.org/api/v2/procnotices"
NOTICE_URL_TEMPLATE  = "https://projects.worldbank.org/en/projects-operations/procurement-detail/{}"
PORTAL               = "worldbank"
PAGE_SIZE            = 500
REQUEST_DELAY        = 2
MAX_RETRIES          = 3
TODAY                = date.today()           # FIX #6 — date object
TODAY_ISO            = TODAY.isoformat()
YEAR_FILTER          = "2026"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


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
    """FIX #6 — returns date object for safe comparison."""
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
    Apply ingestion filters and return parsed row, or None if disqualified.

    Filters:
        - publication_date must be 2026
        - deadline must exist and not have passed
        - title must exist
    """
    pub_date = _parse_date(notice.get("noticedate", ""))
    if not pub_date or str(pub_date.year) != YEAR_FILTER:
        return None

    deadline = _parse_date(notice.get("submission_deadline_date", ""))
    if not deadline:
        return None

    # FIX #6 — date object comparison
    if deadline < TODAY:
        return None

    title = notice.get("project_name", "")
    if not title:
        return None

    notice_id = notice.get("id", "").strip()
    if not notice_id:
        return None

    return {
        "notice_id":               notice_id,
        "title":                   title,
        "notice_text":             notice.get("notice_text") or None,
        "description":             notice.get("bid_description") or None,
        "country":                 notice.get("project_ctry_name") or None,
        "publication_date":        pub_date.isoformat(),
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
        # URL FIX — notice_url is the direct link to this specific notice
        # contact_web_url is the borrower/org website (different thing)
        "notice_url":              NOTICE_URL_TEMPLATE.format(notice_id),
        "contact_web_url":         notice.get("contact_web_url") or None,
        # internal — used for filtering, not stored
        "_deadline_obj":           deadline,
        "_pub_date_obj":           pub_date,
        "notice_text":             notice.get("notice_text") or None,
    }


def parse_page(data: dict) -> tuple[list[dict], int, int]:
    notices  = data.get("procnotices", [])
    active   = []
    expired  = 0
    filtered = 0
    errors   = 0

    for notice in notices:
        try:
            parsed = parse_notice(notice)
            if parsed is None:
                dl = _parse_date(notice.get("submission_deadline_date", ""))
                # FIX #6 — date object comparison
                if dl and dl < TODAY:
                    expired += 1
                else:
                    filtered += 1
            else:
                active.append(parsed)
        except Exception as e:
            logger.warning(f"Failed to parse notice {notice.get('id', '?')}: {e}")
            errors += 1

    logger.info(
        f"Page — active:{len(active)} expired:{expired} "
        f"pre-2026/invalid:{filtered} errors:{errors}"
    )
    return active, expired, filtered


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
        # URL FIX — store the direct notice page URL, not the contact website
        "source_url":              row["notice_url"],
    })


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run(preview: bool = False) -> None:
    started_at = datetime.now(timezone.utc)

    logger.info("=" * 65)
    logger.info("World Bank Historical Scraper — Stage 1 Ingestion")
    logger.info(f"Today       : {TODAY_ISO}")
    logger.info(f"Year filter : publication_date in {YEAR_FILTER}")
    logger.info(f"Mode        : {'PREVIEW — no saves' if preview else 'FULL BACKFILL'}")
    logger.info("=" * 65)

    total_saved    = 0
    total_expired  = 0
    total_filtered = 0
    total_errors   = 0
    offset         = 0
    page_num       = 0

    try:
        while True:
            page_num += 1
            logger.info(f"\n--- Page {page_num} (offset={offset}) ---")

            data = fetch_page(offset)
            if not data:
                logger.error(f"Failed to fetch page {page_num} — stopping")
                break

            notices = data.get("procnotices", [])
            if not notices:
                logger.info("No more records — reached end of API")
                break

            active, expired, filtered = parse_page(data)
            total_expired  += expired
            total_filtered += filtered

            if len(active) == 0 and len(notices) > 0:
                logger.info(
                    f"Full page of {len(notices)} records with zero qualifying tenders — "
                    f"nothing older will have a 2026 publication date, stopping"
                )
                break

            if not preview:
                for row in active:
                    try:
                        saved = _save(row)
                        logger.info(
                            f"  ✅  {saved['title'][:50]:<50} "
                            f"| {saved['country'] or 'N/A':<20} "
                            f"| deadline:{saved['deadline_date']} "
                            f"| url:{saved['source_url']}"          # FIX — show notice URL
                        )
                        total_saved += 1
                    except Exception as e:
                        logger.error(f"  ❌  Save failed: {e} | id={row['notice_id']}")
                        total_errors += 1

                logger.info(f"Page {page_num} — saved:{total_saved} errors:{total_errors}")
            else:
                logger.info(f"PREVIEW — {len(active)} qualifying tenders on this page")
                for i, row in enumerate(active[:5], 1):
                    print(f"\n  [{i}] {row['notice_id']}")
                    print(f"    title        : {row['title'][:60]}")
                    print(f"    country      : {row['country']}")
                    print(f"    published    : {row['publication_date']}")
                    print(f"    deadline     : {row['deadline_date']}")
                    print(f"    notice_type  : {row['notice_type']}")
                    print(f"    notice_url   : {row['notice_url']}")          # FIX
                    print(f"    contact_web  : {row['contact_web_url']}")     # FIX
                    print(f"    notice_text  : {(row['notice_text'] or '')[:120]}...")
                break

            offset += PAGE_SIZE
            time.sleep(REQUEST_DELAY)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        total_errors += 1

    # ── Determine status ──────────────────────────────────────
    if total_errors > 0 and total_saved == 0:
        run_status = "failed"
    elif total_errors > 0:
        run_status = "partial"
    else:
        run_status = "success"

    logger.info("\n" + "=" * 65)
    logger.info(f"Status              : {run_status}")
    logger.info(f"Total saved         : {total_saved}")
    logger.info(f"Total expired       : {total_expired}")
    logger.info(f"Total pre-2026/inv. : {total_filtered}")
    logger.info(f"Total errors        : {total_errors}")
    logger.info(f"Pages fetched       : {page_num}")
    logger.info("=" * 65)
    logger.info("Next step: python enricher_worldbank.py")
    logger.info("=" * 65)

    if not preview:
        # Seed the daily scraper cursor with the newest ID now in DB
        newest_id = get_newest_id_from_db(PORTAL)
        if newest_id:
            save_state(PORTAL, newest_id)
            logger.info(f"Daily scraper cursor seeded → {newest_id}")
        else:
            logger.warning("No tenders saved — cursor not seeded")

        # Persist run stats for dashboard
        log_scraper_run(
            portal          = PORTAL,
            started_at      = started_at,
            status          = run_status,
            saved           = total_saved,
            expired_skipped = total_expired,
            year_filtered   = total_filtered,
            errors          = total_errors,
            new_cursor      = newest_id,
            notes           = "historical backfill",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="World Bank Historical Scraper")
    parser.add_argument("--preview", action="store_true", help="First page only, no saves")
    args = parser.parse_args()
    init_db()
    run(preview=args.preview)