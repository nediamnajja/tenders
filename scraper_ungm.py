"""
scraper_ungm_historical.py — UNGM Historical Scraper
=====================================================
Scrapes ALL tenders published since 01/01/2020 (configurable).

Filter applied:
    txtNoticePublishedFrom = 01/01/2020
    txtNoticePublishedTo   = (blank)
    deadline fields        = (blank — we want everything, expired or not)

Skipped orgs (scraped from their own portals):
    UNDP, AfDB, African Development Bank, World Bank, WBG, IBRD, IDA

Lazy loading:
    UNGM renders rows progressively as you scroll.
    This scraper scrolls the results container until no new rows appear,
    then moves to the next page.

Usage:
    python scraper_ungm_historical.py
    python scraper_ungm_historical.py --preview
    python scraper_ungm_historical.py --start 01/01/2023
"""

import argparse
import logging
import re
import time
from datetime import datetime, date
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
PORTAL = "ungm"
HISTORY_START = "01/01/2026"
TODAY_ISO = date.today().isoformat()

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
#  STEP 1 — LOAD + FILTER
# ─────────────────────────────────────────────────────────────

def _js_set(driver: webdriver.Chrome, field_id: str, value: str) -> None:
    """Set a date input via JS and fire change event so UNGM picks it up."""
    try:
        el = driver.find_element(By.ID, field_id)
        driver.execute_script("arguments[0].value = arguments[1];", el, value)
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", el
        )
        logger.info(f"  #{field_id} = '{value}'")
    except NoSuchElementException:
        logger.warning(f"  Field not found: #{field_id}")


def _click_search(driver: webdriver.Chrome) -> None:
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
    logger.warning("Search button not found — results may use defaults")


def _wait_for_rows(driver: webdriver.Chrome, timeout: int = 20) -> bool:
    """Wait until at least one data row is visible in #tblNotices."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(
                d.find_elements(By.CSS_SELECTOR, "#tblNotices .tableBody .tableRow")
            ) > 0
            or _is_empty(d)
        )
        if _is_empty(driver):
            logger.warning("UNGM returned no results")
            return False
        return True
    except TimeoutException:
        logger.warning("Timed out waiting for rows")
        return False


def _is_empty(driver: webdriver.Chrome) -> bool:
    try:
        return driver.find_element(By.ID, "noticesEmpty").is_displayed()
    except Exception:
        return False


def sort_by_publication_date(driver: webdriver.Chrome) -> None:
    """
    Click 'Date de publication' header twice so results are sorted
    descending (newest first). The column id is 'id_DatePublished'.
    Must be called AFTER filter results load and BEFORE any scrolling.
    """
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

        logger.info("  Sorted by Date de publication ↓ (newest first)")
    except Exception as e:
        logger.warning(f"  Could not sort by publication date: {e}")


def load_and_filter(driver: webdriver.Chrome, published_from: str) -> bool:
    """
    Open the UNGM notices page, set 'Publié entre le' to published_from,
    leave everything else blank, submit, wait for results, then sort by
    publication date descending — all before any scrolling happens.
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

    for fid in ["txtNoticePublishedTo", "txtNoticeDeadlineFrom", "txtNoticeDeadlineTo"]:
        try:
            el = driver.find_element(By.ID, fid)
            driver.execute_script("arguments[0].value = '';", el)
        except NoSuchElementException:
            pass

    _click_search(driver)
    time.sleep(3)

    if not _wait_for_rows(driver, timeout=30):
        return False

    sort_by_publication_date(driver)
    return True


# ─────────────────────────────────────────────────────────────
#  STEP 2 — LAZY-LOAD SCROLL
# ─────────────────────────────────────────────────────────────

def scroll_to_load_all(driver: webdriver.Chrome) -> None:
    """
    UNGM uses lazy loading — rows are injected as you scroll.
    Scroll down in the results container until the row count stops growing.
    """
    logger.info("  Scrolling to load all lazy rows...")
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
                logger.info(f"  Scroll complete — {current_count} rows loaded")
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
#  STEP 3 — EXTRACT ROWS
# ─────────────────────────────────────────────────────────────
#
#  Confirmed column order from live page source:
#    [0] intend button   — skip
#    [1] Titre           — title + <a> link
#    [2] Date d'échéance — deadline
#    [3] Date de publication — published
#    [4] Organisation
#    [5] Type d'avis     — notice type
#    [6] Référence
#    [7] Pays/territoire — country

def _cell(row_el, idx: int) -> str:
    try:
        cells = row_el.find_elements(By.CSS_SELECTOR, ".tableCell")
        return cells[idx].text.strip() if idx < len(cells) else ""
    except Exception:
        return ""


def _extract_title_and_url(driver: webdriver.Chrome, title_cell) -> tuple[str, str, str]:
    """
    Extract the tender title, detail URL, and notice_id from the title cell.

    Strategy:
      1. Read the visible title from `.ungm-title`
      2. Find the real notice link via href matching /Public/Notice/<digits>
      3. Fall back to useful link text
      4. Fall back to cleaned innerText
    """
    title = ""
    detail_url = ""
    notice_id = ""

    UI_LABELS = {
        "open in new window",
        "open",
        "new window",
        "view",
        "open, a new window",
    }

    try:
        # 1) Best source for the title: visible title span
        try:
            title_span = title_cell.find_element(By.CSS_SELECTOR, ".ungm-title")
            title = (title_span.text or "").strip()
        except NoSuchElementException:
            pass

        # 2) Best source for the detail URL: href pattern
        links = title_cell.find_elements(By.TAG_NAME, "a")
        notice_link = None

        for link in links:
            href = (link.get_attribute("href") or "").strip()
            if re.search(r"/Public/Notice/\d+", href):
                notice_link = link
                break

        if notice_link is not None:
            href = (notice_link.get_attribute("href") or "").strip()
            if href.startswith("http"):
                detail_url = href
            elif href:
                detail_url = "https://www.ungm.org" + href

        # 3) If title still missing, try link text excluding UI labels
        if not title:
            for link in links:
                txt = (link.text or "").strip()
                if txt and txt.lower() not in UI_LABELS:
                    title = txt
                    break

        # 4) Final fallback: use cell text, minus UI helper lines
        if not title:
            raw = (
                driver.execute_script("return arguments[0].innerText;", title_cell) or ""
            ).strip()
            lines = []
            for line in raw.splitlines():
                clean = line.strip()
                if clean and clean.lower() not in UI_LABELS:
                    lines.append(clean)
            title = " ".join(lines).strip()

        if detail_url:
            m = re.search(r"/Public/Notice/(\d+)", detail_url)
            if m:
                notice_id = m.group(1)

        if title:
            title = re.sub(
                r"\bOpen,?\s*a\s*new\s*window\b", "", title, flags=re.I
            ).strip(" -–|")

    except Exception as e:
        logger.debug(f"Title extraction error: {e}")

    return title, detail_url, notice_id


def extract_rows(driver: webdriver.Chrome) -> list[dict]:
    results = []
    skipped_org = 0
    errors = 0

    rows = driver.find_elements(
        By.CSS_SELECTOR, "#tblNotices .tableBody .tableRow"
    )
    logger.info(f"  Extracting {len(rows)} rows")

    for row in rows:
        try:
            title = ""
            detail_url = ""
            notice_id = ""

            cells = row.find_elements(By.CSS_SELECTOR, ".tableCell")

            if len(cells) > 1:
                title, detail_url, notice_id = _extract_title_and_url(driver, cells[1])

            # Last-resort: pull notice_id from row onclick
            if not title and not notice_id:
                onclick = row.get_attribute("onclick") or ""
                m = re.search(r"/Public/Notice/(\d+)", onclick)
                if m:
                    notice_id = m.group(1)
                    detail_url = f"https://www.ungm.org/Public/Notice/{notice_id}"
                else:
                    continue

            deadline = _parse_date(_cell(row, 2))
            posted = _parse_date(_cell(row, 3))

            org_name = _cell(row, 4)
            if not org_name:
                org_name = "Unknown"

            if org_name in SKIP_ORGS:
                logger.debug(f"  ⏭  {org_name} skipped")
                skipped_org += 1
                continue

            notice_type = _cell(row, 5)
            ref_no = _cell(row, 6)

            if len(cells) > 7:
                country_cell = cells[7]
                raw_country = driver.execute_script(
                    "return arguments[0].innerText;", country_cell
                ).strip()
                country_parts = [
                    p.strip() for p in raw_country.splitlines() if p.strip()
                ]
                raw_country = ", ".join(country_parts)
            else:
                raw_country = ""

            country = raw_country if raw_country not in ("", "-", "N/A") else None

            results.append({
                "notice_id": notice_id or ref_no or title[:30],
                "title": title,
                "ref_no": ref_no,
                "org_name": org_name,
                "notice_type": notice_type,
                "deadline": deadline,
                "posted": posted,
                "source_url": detail_url,
                "country": country,
                "description": None,
                "contact": None,
            })

        except StaleElementReferenceException:
            errors += 1
        except Exception as e:
            logger.warning(f"Row error: {e}")
            errors += 1

    logger.info(f"  → kept:{len(results)}  skipped_org:{skipped_org}  errors:{errors}")
    return results


# ─────────────────────────────────────────────────────────────
#  STEP 4 — DETAIL PAGE
# ─────────────────────────────────────────────────────────────

def scrape_detail(driver: webdriver.Chrome, url: str) -> dict:
    result = {"description": None, "contact": None}
    if not url:
        return result
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(1)

        for sel in [
            "#noticeDescription",
            ".notice-description",
            "#Description",
            "[id*='escription']",
        ]:
            try:
                txt = driver.find_element(By.CSS_SELECTOR, sel).text.strip()
                if txt:
                    result["description"] = txt
                    break
            except NoSuchElementException:
                continue

        for sel in [
            "#contactInformation",
            ".contact-information",
            "#Contact",
            "[id*='ontact']",
        ]:
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

def go_to_next_page(driver: webdriver.Chrome) -> bool:
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
#  STEP 5 — SAVE
# ─────────────────────────────────────────────────────────────

def _save(row: dict) -> None:
    org_id = upsert_organisation(
        name=row["org_name"],
        organisation_type="International",
        country=row["country"],
    )

    upsert_tender({
        "source_portal": PORTAL,
        "tender_id": row["notice_id"],
        "title": row["title"],
        "description": row.get("description"),
        "country": row.get("country"),
        "notice_type": row.get("notice_type"),
        "language": "English",
        "publication_date": row.get("posted"),
        "deadline_date": row.get("deadline"),
        "deadline_time": None,
        "budget": None,
        "currency": None,
        "project_id": row.get("ref_no"),
        "procurement_group": None,
        "procurement_method_code": None,
        "procurement_method_name": None,
        "sector": None,
        "keywords": None,
        "summary": None,
        "contact": row.get("contact"),
        "status_id": 4,
        "organisation_id": org_id,
        "source_url": row.get("source_url"),
    })


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run(preview: bool = False, start_date: str = "") -> None:
    published_from = start_date or HISTORY_START

    logger.info("=" * 70)
    logger.info("UNGM HISTORICAL SCRAPER")
    logger.info(f"Published from : {published_from}  →  (no end date)")
    logger.info("Deadline filter: blank (scrape all, expired or not)")
    logger.info(f"Skip orgs      : {', '.join(sorted(SKIP_ORGS))}")
    logger.info(f"Preview        : {'YES — no saves' if preview else 'NO'}")
    logger.info("=" * 70)

    driver = create_driver()
    stats = {"saved": 0, "errors": 0, "pages": 0, "newest_id": None}

    try:
        if not load_and_filter(driver, published_from):
            logger.error("Could not load results — aborting")
            return

        page_num = 0

        while True:
            page_num += 1
            stats["pages"] += 1
            logger.info(f"\n{'─' * 60}")
            logger.info(f"PAGE {page_num}")
            logger.info(f"{'─' * 60}")

            scroll_to_load_all(driver)
            rows = extract_rows(driver)

            if not rows and page_num == 1:
                logger.warning("No rows on first page — saving debug HTML")
                with open("ungm_historical_debug.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                logger.info("Saved → ungm_historical_debug.html")
                break

            for row in rows:
                notice_id = row["notice_id"]

                if row["source_url"] and not preview:
                    detail = scrape_detail(driver, row["source_url"])
                    row["description"] = detail["description"]
                    row["contact"] = detail["contact"]
                    driver.back()
                    time.sleep(2)
                    _wait_for_rows(driver, timeout=10)

                if preview:
                    print(f"  notice_id  : {notice_id}")
                    print(f"  title      : {row['title'][:70]}")
                    print(f"  org        : {row['org_name']}")
                    print(f"  country    : {row['country']}")
                    print(f"  reference  : {row['ref_no']}")
                    print(f"  published  : {row['posted']}")
                    print(f"  deadline   : {row['deadline']}")
                    print(f"  type       : {row['notice_type']}")
                    print(f"  url        : {row['source_url']}")
                    print()
                    stats["saved"] += 1
                    stats["newest_id"] = stats["newest_id"] or notice_id
                    continue

                try:
                    _save(row)
                    logger.info(
                        f"  ✅  {row['title'][:50]:<50}"
                        f" | {row['org_name']:<12}"
                        f" | {row['deadline'] or 'no deadline'}"
                    )
                    stats["saved"] += 1
                    stats["newest_id"] = stats["newest_id"] or notice_id
                except Exception as e:
                    logger.error(f"  ❌  [{notice_id}]: {e}")
                    stats["errors"] += 1

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

    if not preview and stats["newest_id"]:
        with get_session() as session:
            state = session.query(ScraperState).filter_by(portal=PORTAL).first()
            if state:
                state.last_run = TODAY_ISO
                state.last_notice_id = stats["newest_id"]
            else:
                session.add(ScraperState(
                    portal=PORTAL,
                    last_run=TODAY_ISO,
                    last_notice_id=stats["newest_id"],
                ))

    logger.info("\n" + "=" * 70)
    logger.info("HISTORICAL SCRAPE COMPLETE")
    logger.info(f"  Pages  : {stats['pages']}")
    logger.info(f"  Saved  : {stats['saved']}")
    logger.info(f"  Errors : {stats['errors']}")
    logger.info("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UNGM Historical Scraper")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="First page only, no DB writes"
    )
    parser.add_argument(
        "--start",
        default="",
        metavar="DD/MM/YYYY",
        help=f"Start date (default: {HISTORY_START})"
    )
    args = parser.parse_args()
    init_db()
    run(preview=args.preview, start_date=args.start)