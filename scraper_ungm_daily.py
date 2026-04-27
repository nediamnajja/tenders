"""
scraper_ungm_daily.py — UNGM Daily Scraper
===========================================
Runs every day via Task Scheduler.

Dual cursor for precise incremental tracking:
  1. last_notice_id  → primary cursor — stops immediately when we hit
                       a notice_id we already have in the DB
  2. last_run_date   → safety net — if scraper was offline for N days,
                       filters published_from = last_run_date so we
                       catch everything missed

Logic:
  - On first ever run → falls back to yesterday as published_from
  - On normal run     → sets published_from = last_run_date from ScraperState
  - Stops scraping    → as soon as notice_id matches last_notice_id (already known)
  - Saves state       → updates last_run and last_notice_id after every run

Skipped orgs (scraped from their own portals):
    UNDP, AfDB, African Development Bank, World Bank, WBG, IBRD, IDA

Usage:
    python scraper_ungm_daily.py
    python scraper_ungm_daily.py --preview
"""

import argparse
import logging
import re
import time
from datetime import datetime, date, timedelta
from typing import Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from webdriver_manager.chrome import ChromeDriverManager

from db import init_db, get_session, upsert_organisation, upsert_tender
from models import ScraperState

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────

TENDER_URL = "https://www.ungm.org/Public/Notice"
PORTAL     = "ungm"
TODAY_ISO  = date.today().isoformat()
YESTERDAY  = (date.today() - timedelta(days=1)).strftime("%d/%m/%Y")

SKIP_ORGS = {
    "UNDP",
    "AfDB",
    "African Development Bank",
    "World Bank",
    "WBG",
    "IBRD",
    "IDA",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  DUAL CURSOR — load from DB
# ─────────────────────────────────────────────────────────────

def load_cursors() -> tuple[str, str]:
    """
    Load last_run_date and last_notice_id from ScraperState.
    Returns (published_from, last_notice_id).

    published_from  — used as the UNGM date filter (DD/MM/YYYY)
    last_notice_id  — primary stop cursor
    """
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=PORTAL).first()

        if not state or not state.last_run:
            # First ever run — use yesterday as safety net
            logger.info("No previous state found — first run, using yesterday as start date")
            return YESTERDAY, None

        # Convert stored ISO date (YYYY-MM-DD) → UNGM format (DD/MM/YYYY)
        try:
            last_run_dt     = datetime.strptime(state.last_run, "%Y-%m-%d")
            published_from  = last_run_dt.strftime("%d/%m/%Y")
        except ValueError:
            published_from  = YESTERDAY

        last_notice_id = state.last_notice_id or None

        logger.info(f"Cursors loaded — published_from={published_from}, last_notice_id={last_notice_id}")
        return published_from, last_notice_id


def save_cursors(newest_notice_id: str) -> None:
    """Update ScraperState with today's run date and newest notice_id seen."""
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=PORTAL).first()
        if state:
            state.last_run       = TODAY_ISO
            state.last_notice_id = newest_notice_id
        else:
            session.add(ScraperState(
                portal         = PORTAL,
                last_run       = TODAY_ISO,
                last_notice_id = newest_notice_id,
            ))
        session.commit()
    logger.info(f"Cursors saved — last_run={TODAY_ISO}, last_notice_id={newest_notice_id}")


# ─────────────────────────────────────────────────────────────
#  BROWSER
# ─────────────────────────────────────────────────────────────

def create_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


# ─────────────────────────────────────────────────────────────
#  FILTER + LOAD
# ─────────────────────────────────────────────────────────────

def _js_set(driver, field_id: str, value: str) -> None:
    try:
        el = driver.find_element(By.ID, field_id)
        driver.execute_script("arguments[0].value = arguments[1];", el, value)
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", el
        )
        logger.info(f"  #{field_id} = '{value}'")
    except NoSuchElementException:
        logger.warning(f"  Field not found: #{field_id}")


def _click_search(driver) -> None:
    for sel in [
        "#btnSearch",
        "button[type='submit']",
        "input[type='submit']",
        ".filterButtons button",
        "#noticeFilter button",
    ]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            if btn.is_displayed():
                btn.click()
                logger.info(f"Search clicked via '{sel}'")
                return
        except NoSuchElementException:
            continue
    logger.warning("Search button not found")


def _wait_for_rows(driver, timeout: int = 20) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(
                d.find_elements(By.CSS_SELECTOR, "#tblNotices .tableBody .tableRow")
            ) > 0 or _is_empty(d)
        )
        return not _is_empty(driver)
    except TimeoutException:
        logger.warning("Timed out waiting for rows")
        return False


def _is_empty(driver) -> bool:
    try:
        return driver.find_element(By.ID, "noticesEmpty").is_displayed()
    except Exception:
        return False


def sort_by_publication_date(driver) -> None:
    try:
        header = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "id_DatePublished"))
        )
        header.click()
        time.sleep(2)
        _wait_for_rows(driver, timeout=15)
        header = driver.find_element(By.ID, "id_DatePublished")
        header.click()
        time.sleep(2)
        _wait_for_rows(driver, timeout=15)
        logger.info("  Sorted by publication date (newest first)")
    except Exception as e:
        logger.warning(f"  Could not sort: {e}")


def load_and_filter(driver, published_from: str) -> bool:
    """
    Open UNGM, set published_from date filter, submit, sort newest first.
    Daily scraper sets published_from = last_run_date so we only fetch
    tenders published since the last run.
    """
    logger.info(f"Loading {TENDER_URL} ...")
    driver.get(TENDER_URL)

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "txtNoticePublishedFrom"))
        )
    except TimeoutException:
        logger.error("Filter form did not appear")
        return False

    time.sleep(1)
    _js_set(driver, "txtNoticePublishedFrom", published_from)

    # Clear all other date filters — daily scraper only filters by published date
    for fid in ["txtNoticePublishedTo", "txtNoticeDeadlineFrom", "txtNoticeDeadlineTo"]:
        try:
            el = driver.find_element(By.ID, fid)
            driver.execute_script("arguments[0].value = '';", el)
        except NoSuchElementException:
            pass

    _click_search(driver)
    time.sleep(3)

    if not _wait_for_rows(driver, timeout=30):
        logger.info("No new tenders found since last run")
        return False

    sort_by_publication_date(driver)
    return True


# ─────────────────────────────────────────────────────────────
#  LAZY SCROLL
# ─────────────────────────────────────────────────────────────

def scroll_to_load_all(driver) -> None:
    logger.info("  Scrolling to load all rows...")
    last_count = 0
    stall = 0

    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)

        try:
            container = driver.find_element(By.ID, "tblNotices")
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight;", container
            )
        except NoSuchElementException:
            pass

        time.sleep(1)
        current_count = len(
            driver.find_elements(By.CSS_SELECTOR, "#tblNotices .tableBody .tableRow")
        )

        if current_count == last_count:
            stall += 1
            if stall >= 3:
                logger.info(f"  Scroll done — {current_count} rows loaded")
                break
        else:
            logger.info(f"  Rows so far: {current_count}")
            stall = 0

        last_count = current_count


# ─────────────────────────────────────────────────────────────
#  DATE HELPER
# ─────────────────────────────────────────────────────────────

def _parse_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip().split()[0]
    for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ─────────────────────────────────────────────────────────────
#  EXTRACT ROWS
# ─────────────────────────────────────────────────────────────

def _cell(row_el, idx: int) -> str:
    try:
        cells = row_el.find_elements(By.CSS_SELECTOR, ".tableCell")
        return cells[idx].text.strip() if idx < len(cells) else ""
    except Exception:
        return ""


def _extract_title_and_url(driver, title_cell) -> tuple[str, str, str]:
    title      = ""
    detail_url = ""
    notice_id  = ""

    UI_LABELS = {"open in new window", "open", "new window", "view", "open, a new window"}

    try:
        try:
            title_span = title_cell.find_element(By.CSS_SELECTOR, ".ungm-title")
            title = (title_span.text or "").strip()
        except NoSuchElementException:
            pass

        links       = title_cell.find_elements(By.TAG_NAME, "a")
        notice_link = None

        for link in links:
            href = (link.get_attribute("href") or "").strip()
            if re.search(r"/Public/Notice/\d+", href):
                notice_link = link
                break

        if notice_link is not None:
            href = (notice_link.get_attribute("href") or "").strip()
            detail_url = href if href.startswith("http") else "https://www.ungm.org" + href

        if not title:
            for link in links:
                txt = (link.text or "").strip()
                if txt and txt.lower() not in UI_LABELS:
                    title = txt
                    break

        if not title:
            raw = (driver.execute_script("return arguments[0].innerText;", title_cell) or "").strip()
            lines = [l.strip() for l in raw.splitlines() if l.strip() and l.strip().lower() not in UI_LABELS]
            title = " ".join(lines).strip()

        if detail_url:
            m = re.search(r"/Public/Notice/(\d+)", detail_url)
            if m:
                notice_id = m.group(1)

        if title:
            title = re.sub(r"\bOpen,?\s*a\s*new\s*window\b", "", title, flags=re.I).strip(" -–|")

    except Exception as e:
        logger.debug(f"Title extraction error: {e}")

    return title, detail_url, notice_id


def extract_rows(driver, last_notice_id: str) -> tuple[list[dict], bool]:
    """
    Extract rows from current page.
    Returns (rows_list, stop_flag).

    stop_flag = True means we hit last_notice_id — caller should stop pagination.
    Sorted newest first, so we stop as soon as we see a known notice_id.
    """
    results    = []
    skipped    = 0
    errors     = 0
    stop_flag  = False

    rows = driver.find_elements(By.CSS_SELECTOR, "#tblNotices .tableBody .tableRow")
    logger.info(f"  Extracting {len(rows)} rows on this page")

    for row in rows:
        try:
            cells = row.find_elements(By.CSS_SELECTOR, ".tableCell")
            title, detail_url, notice_id = "", "", ""

            if len(cells) > 1:
                title, detail_url, notice_id = _extract_title_and_url(driver, cells[1])

            if not notice_id:
                onclick = row.get_attribute("onclick") or ""
                m = re.search(r"/Public/Notice/(\d+)", onclick)
                if m:
                    notice_id  = m.group(1)
                    detail_url = f"https://www.ungm.org/Public/Notice/{notice_id}"
                else:
                    continue

            # ── PRIMARY CURSOR CHECK ─────────────────────────────────
            # Results are sorted newest first — stop as soon as we see
            # a notice_id we already processed in the last run
            if last_notice_id and notice_id == last_notice_id:
                logger.info(f"  🛑 Hit last_notice_id={last_notice_id} — stopping")
                stop_flag = True
                break
            # ────────────────────────────────────────────────────────

            deadline   = _parse_date(_cell(row, 2))
            posted     = _parse_date(_cell(row, 3))
            org_name   = _cell(row, 4) or "Unknown"

            if org_name in SKIP_ORGS:
                skipped += 1
                continue

            notice_type = _cell(row, 5)
            ref_no      = _cell(row, 6)

            if len(cells) > 7:
                raw_country  = driver.execute_script(
                    "return arguments[0].innerText;", cells[7]
                ).strip()
                country_parts = [p.strip() for p in raw_country.splitlines() if p.strip()]
                raw_country   = ", ".join(country_parts)
            else:
                raw_country = ""

            country = raw_country if raw_country not in ("", "-", "N/A") else None

            results.append({
                "notice_id":   notice_id or ref_no or title[:30],
                "title":       title,
                "ref_no":      ref_no,
                "org_name":    org_name,
                "notice_type": notice_type,
                "deadline":    deadline,
                "posted":      posted,
                "source_url":  detail_url,
                "country":     country,
                "description": None,
                "contact":     None,
            })

        except StaleElementReferenceException:
            errors += 1
        except Exception as e:
            logger.warning(f"Row error: {e}")
            errors += 1

    logger.info(f"  → kept:{len(results)}  skipped_org:{skipped}  errors:{errors}")
    return results, stop_flag


# ─────────────────────────────────────────────────────────────
#  DETAIL PAGE
# ─────────────────────────────────────────────────────────────

def scrape_detail(driver, url: str) -> dict:
    result = {"description": None, "contact": None}
    if not url:
        return result
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1)

        for sel in ["#noticeDescription", ".notice-description", "#Description", "[id*='escription']"]:
            try:
                txt = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
                if txt:
                    result["description"] = txt
                    break
            except NoSuchElementException:
                continue

        for sel in ["#contactInformation", ".contact-information", "#Contact", "[id*='ontact']"]:
            try:
                txt = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
                if txt:
                    result["contact"] = txt
                    break
            except NoSuchElementException:
                continue

    except TimeoutException:
        logger.warning(f"Timeout: {url}")
    except Exception as e:
        logger.warning(f"Detail error: {e}")
    return result


# ─────────────────────────────────────────────────────────────
#  PAGINATION
# ─────────────────────────────────────────────────────────────

def go_to_next_page(driver) -> bool:
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.5)

    for sel in [
        "li.next:not(.disabled) a",
        "a[aria-label='Next']",
        ".pagination .next:not(.disabled) a",
        "a.nextLink",
        "[data-page='next']",
    ]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            if btn.is_displayed() and btn.is_enabled():
                btn.click()
                time.sleep(3)
                _wait_for_rows(driver, timeout=15)
                return True
        except Exception:
            continue
    return False


# ─────────────────────────────────────────────────────────────
#  SAVE
# ─────────────────────────────────────────────────────────────

def _save(row: dict) -> None:
    org_id = upsert_organisation(
        name=row["org_name"],
        organisation_type="International",
        country=row["country"],
    )
    upsert_tender({
        "source_portal":          PORTAL,
        "tender_id":              row["notice_id"],
        "title":                  row["title"],
        "description":            row.get("description"),
        "country":                row.get("country"),
        "notice_type":            row.get("notice_type"),
        "language":               "English",
        "publication_date":       row.get("posted"),
        "deadline_date":          row.get("deadline"),
        "deadline_time":          None,
        "budget":                 None,
        "currency":               None,
        "project_id":             row.get("ref_no"),
        "procurement_group":      None,
        "procurement_method_code": None,
        "procurement_method_name": None,
        "sector":                 None,
        "keywords":               None,
        "summary":                None,
        "contact":                row.get("contact"),
        "status_id":              4,
        "organisation_id":        org_id,
        "source_url":             row.get("source_url"),
    })


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run(preview: bool = False) -> None:
    # ── Load dual cursors ────────────────────────────────────
    published_from, last_notice_id = load_cursors()

    logger.info("=" * 70)
    logger.info("UNGM DAILY SCRAPER")
    logger.info(f"Published from  : {published_from}  (last_run_date cursor)")
    logger.info(f"Stop cursor     : last_notice_id = {last_notice_id or 'none (first run)'}")
    logger.info(f"Skip orgs       : {', '.join(sorted(SKIP_ORGS))}")
    logger.info(f"Preview         : {'YES — no saves' if preview else 'NO'}")
    logger.info("=" * 70)

    driver = create_driver()
    stats  = {"saved": 0, "errors": 0, "pages": 0, "newest_id": None}

    try:
        if not load_and_filter(driver, published_from):
            logger.info("No new tenders since last run — nothing to do")
            return

        page_num = 0

        while True:
            page_num += 1
            stats["pages"] += 1
            logger.info(f"\n{'─'*60}")
            logger.info(f"PAGE {page_num}")
            logger.info(f"{'─'*60}")

            scroll_to_load_all(driver)
            rows, stop_flag = extract_rows(driver, last_notice_id)

            if not rows and page_num == 1:
                logger.info("No new rows found on first page — up to date")
                break

            for row in rows:
                notice_id = row["notice_id"]

                # Track newest notice_id seen this run (first one = newest, sorted desc)
                if stats["newest_id"] is None:
                    stats["newest_id"] = notice_id

                if row["source_url"] and not preview:
                    detail = scrape_detail(driver, row["source_url"])
                    row["description"] = detail["description"]
                    row["contact"]     = detail["contact"]
                    driver.back()
                    time.sleep(2)
                    _wait_for_rows(driver, timeout=10)

                if preview:
                    print(f"  notice_id  : {notice_id}")
                    print(f"  title      : {row['title'][:70]}")
                    print(f"  org        : {row['org_name']}")
                    print(f"  country    : {row['country']}")
                    print(f"  published  : {row['posted']}")
                    print(f"  deadline   : {row['deadline']}")
                    print(f"  url        : {row['source_url']}")
                    print()
                    stats["saved"] += 1
                    continue

                try:
                    _save(row)
                    logger.info(
                        f"  ✅  {row['title'][:50]:<50}"
                        f" | {row['org_name']:<12}"
                        f" | {row['deadline'] or 'no deadline'}"
                    )
                    stats["saved"] += 1
                except Exception as e:
                    logger.error(f"  ❌  [{notice_id}]: {e}")
                    stats["errors"] += 1

            # ── STOP CONDITIONS ──────────────────────────────
            if stop_flag:
                logger.info("Primary cursor hit — stopping pagination")
                break

            if preview:
                logger.info("Preview — stopping after first page")
                break

            if not go_to_next_page(driver):
                logger.info("No more pages — done")
                break

    except Exception as e:
        logger.error(f"Fatal: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()

    # ── Save cursors after successful run ────────────────────
    if not preview and stats["newest_id"]:
        save_cursors(stats["newest_id"])
    elif not preview and not stats["newest_id"]:
        # Nothing new found — still update last_run so date cursor advances
        save_cursors(last_notice_id or "")

    logger.info("\n" + "=" * 70)
    logger.info("DAILY SCRAPE COMPLETE")
    logger.info(f"  Pages    : {stats['pages']}")
    logger.info(f"  Saved    : {stats['saved']}")
    logger.info(f"  Errors   : {stats['errors']}")
    logger.info(f"  Newest   : {stats['newest_id'] or 'none'}")
    logger.info("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UNGM Daily Scraper")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="First page only, no DB writes, print rows to terminal"
    )
    args = parser.parse_args()
    init_db()
    run(preview=args.preview)