"""
scraper_afdb_daily.py — AFDB Daily Scraper
===========================================
Stage 1 — Ingestion layer. Runs every day via scheduler.

Pipeline position:
    THIS FILE → PDFs on disk + metadata in SQLite → enricher_afdb.py

How it works:
    - Starts from page 1 of the listing (newest notices first)
    - For each notice:
        if URL already in DB  → STOP (everything below already saved)
        if pub_date < 2026    → STOP (too old)
        if deadline passed    → SKIP
        else                  → scrape notice page + download PDF + save
    - Updates checkpoint (last_notice_id) after successful run

Usage:
    python scraper_afdb_daily.py           (normal daily run)
    python scraper_afdb_daily.py --reset   (clear checkpoint, re-fetch from scratch)
"""

import argparse
import logging
import re
import subprocess
import time
import random
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

from db import init_db, get_session, upsert_organisation, upsert_tender, get_tender_by_ref
from models import ScraperState

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
LISTING_URL  = "https://www.afdb.org/en/projects-and-operations/procurement"
DOWNLOAD_DIR = Path(r"C:\tenders_data\afdb_pdfs")
PORTAL       = "afdb"
TODAY        = date.today()
TODAY_ISO    = TODAY.isoformat()
YEAR_FILTER  = "2026"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────────────────────────

def get_state() -> dict:
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=PORTAL).first()
        if state:
            return {"last_url": state.last_notice_id, "last_run": state.last_run}
        return {"last_url": None, "last_run": None}


def save_state(last_url: str) -> None:
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=PORTAL).first()
        if state:
            state.last_notice_id = last_url
            state.last_run       = TODAY_ISO
        else:
            state = ScraperState(portal=PORTAL, last_run=TODAY_ISO, last_notice_id=last_url)
            session.add(state)
    logger.info(f"Checkpoint saved — last_url: {last_url}")


def reset_state() -> dict:
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=PORTAL).first()
        if state:
            state.last_notice_id = None
            state.last_run       = TODAY_ISO
        else:
            session.add(ScraperState(portal=PORTAL, last_run=TODAY_ISO))
    logger.info("Checkpoint reset")
    return {"last_url": None, "last_run": None}


# ─────────────────────────────────────────────────────────────
#  CHROME VERSION DETECTION
# ─────────────────────────────────────────────────────────────

def get_chrome_version() -> Optional[int]:
    for hive in [
        r'HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon',
        r'HKEY_LOCAL_MACHINE\Software\Google\Chrome\BLBeacon',
    ]:
        try:
            result = subprocess.run(
                f'reg query "{hive}" /v version',
                capture_output=True, text=True, shell=True
            )
            match = re.search(r"(\d+)\.\d+\.\d+\.\d+", result.stdout)
            if match:
                return int(match.group(1))
        except Exception:
            continue
    logger.warning("Could not detect Chrome version — letting undetected_chromedriver guess")
    return None


# ─────────────────────────────────────────────────────────────
#  DRIVER
# ─────────────────────────────────────────────────────────────

def build_driver():
    options = uc.ChromeOptions()
    prefs = {
        "download.default_directory":         str(DOWNLOAD_DIR.resolve()),
        "download.prompt_for_download":       False,
        "download.directory_upgrade":         True,
        "plugins.always_open_pdf_externally": True,
    }
    options.add_experimental_option("prefs", prefs)
    version = get_chrome_version()
    if version:
        logger.info(f"Detected Chrome version: {version}")
    driver = uc.Chrome(options=options, headless=False, version_main=version)
    driver.set_page_load_timeout(60)
    return driver


def safe_get(driver, url: str) -> bool:
    try:
        driver.get(url)
        return True
    except Exception as e:
        logger.warning(f"⚠️  Browser navigation failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────
#  PDF DOWNLOAD HELPERS
# ─────────────────────────────────────────────────────────────

def list_files() -> set:
    return {p.name for p in DOWNLOAD_DIR.iterdir() if p.is_file()}


def wait_for_download(before: set, expected_name: str, timeout: int = 30) -> Optional[Path]:
    end = time.time() + timeout
    while time.time() < end:
        current   = list_files()
        completed = [f for f in (current - before) if not f.endswith(".crdownload")]
        if completed:
            for f in completed:
                if expected_name.lower() in f.lower():
                    return DOWNLOAD_DIR / f
            return DOWNLOAD_DIR / completed[0]
        time.sleep(1)
    return None


# ─────────────────────────────────────────────────────────────
#  DATE HELPERS
# ─────────────────────────────────────────────────────────────

def _parse_date(text: str) -> Optional[date]:
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%B %d, %Y", "%d-%b-%y"):
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _to_iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


def _is_2026(d: Optional[date]) -> bool:
    return bool(d and d.year == 2026)


# ─────────────────────────────────────────────────────────────
#  TENDER ID
# ─────────────────────────────────────────────────────────────

def _url_to_tender_id(url: str) -> str:
    """URL path used as unique ID — always unique per notice."""
    return re.sub(r"^https?://[^/]+", "", url).strip("/")


# ─────────────────────────────────────────────────────────────
#  SCRAPE LISTING PAGE
# ─────────────────────────────────────────────────────────────

def scrape_listing(driver) -> list[dict]:
    """Extract notice metadata from the current listing page."""
    time.sleep(random.uniform(3, 5))
    notices = []

    rows = driver.find_elements(By.CSS_SELECTOR, ".views-field-field-publication-date")

    for row in rows:
        try:
            parent       = driver.execute_script("return arguments[0].parentElement", row)
            date_el      = row.find_element(By.CSS_SELECTOR, ".date-display-single")
            date_iso_str = (date_el.get_attribute("content") or "").split("T")[0]
            pub_date     = _parse_date(date_iso_str)
            link         = parent.find_element(By.CSS_SELECTOR, ".views-field-title a")
            raw_title    = link.text.strip()
            href         = link.get_attribute("href") or ""

            if not href or not raw_title:
                continue

            url = href if href.startswith("http") else f"https://www.afdb.org{href}"

            notices.append({
                "title":    raw_title,
                "url":      url,
                "pub_date": pub_date,
                "date_iso": _to_iso(pub_date),
            })

        except Exception as e:
            logger.warning(f"  Row parse error: {e}")
            continue

    return notices


# ─────────────────────────────────────────────────────────────
#  SCRAPE NOTICE PAGE
# ─────────────────────────────────────────────────────────────

def scrape_notice_page(driver, notice_url: str, tender_id: str) -> dict:
    """
    Opens one notice page.
    Extracts deadline, description, keywords, and downloads PDF.
    Saves even if PDF missing.
    """
    if not safe_get(driver, notice_url):
        return {
            "deadline":       None,
            "deadline_iso":   None,
            "description":    None,
            "keywords":       None,
            "pdf_path":       None,
            "pdf_downloaded": False,
        }

    time.sleep(random.uniform(4, 7))

    result = {
        "deadline":       None,
        "deadline_iso":   None,
        "description":    None,
        "keywords":       None,
        "pdf_path":       None,
        "pdf_downloaded": False,
    }

    # Description
    try:
        desc_el           = driver.find_element(By.CSS_SELECTOR, ".field-name-body .field-item")
        result["description"] = desc_el.text.strip() or None
    except Exception:
        pass

    # Deadline from labeled fields
    try:
        field_labels = driver.find_elements(
            By.CSS_SELECTOR, ".field-label, .views-label, dt, th"
        )
        for label_el in field_labels:
            label_text = label_el.text.strip().lower()
            try:
                value_el = driver.execute_script(
                    "return arguments[0].nextElementSibling;", label_el
                )
                value = value_el.text.strip() if value_el else ""
            except Exception:
                value = ""
            if not value:
                continue
            if any(kw in label_text for kw in ["deadline", "closing", "submission"]):
                result["deadline"]     = value
                result["deadline_iso"] = _to_iso(_parse_date(value))
                break
    except Exception:
        pass

    # Deadline fallback — body text scan
    if not result["deadline"]:
        try:
            lines = driver.find_element(By.TAG_NAME, "body").text.split("\n")
            for i, line in enumerate(lines):
                if any(kw in line.lower() for kw in ["deadline", "closing date", "submission deadline"]):
                    for candidate in [line] + (lines[i+1:i+3] if i + 1 < len(lines) else []):
                        parsed_dl = _parse_date(candidate)
                        if parsed_dl:
                            result["deadline"]     = candidate.strip()
                            result["deadline_iso"] = _to_iso(parsed_dl)
                            break
        except Exception:
            pass

    # Keywords from Related Sections
    try:
        kw_section = driver.find_element(By.CSS_SELECTOR, "#block-views-keywords-block")
        kw_links   = kw_section.find_elements(By.CSS_SELECTOR, "ul li a")
        keywords   = [a.text.strip() for a in kw_links if a.text.strip()]
        result["keywords"] = ", ".join(keywords) if keywords else None
    except Exception:
        pass

    # PDF download — deterministic filename from tender_id
    safe_id           = re.sub(r"[^a-z0-9_\-]", "_", tender_id.lower())[:100]
    expected_filename = f"{safe_id}.pdf"
    expected_path     = DOWNLOAD_DIR / expected_filename

    if expected_path.exists():
        logger.info(f"    ♻️  PDF already on disk: {expected_filename}")
        result["pdf_path"]       = str(expected_path.resolve())
        result["pdf_downloaded"] = True
    else:
        try:
            all_links = driver.find_elements(By.TAG_NAME, "a")
            pdf_links = [
                a for a in all_links
                if "pdf" in (a.get_attribute("href") or "").lower()
                or "pdf" in (a.get_attribute("class") or "").lower()
            ]
            if pdf_links:
                before = list_files()
                target = pdf_links[0]
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
                time.sleep(random.uniform(0.5, 1.5))
                driver.execute_script("arguments[0].click();", target)
                downloaded = wait_for_download(before, expected_filename, timeout=30)
                if downloaded:
                    final_path = DOWNLOAD_DIR / expected_filename
                    try:
                        if downloaded.resolve() != final_path.resolve():
                            if final_path.exists():
                                final_path.unlink()
                            downloaded.rename(final_path)
                        result["pdf_path"]       = str(final_path.resolve())
                        result["pdf_downloaded"] = True
                        logger.info(f"    ✅ PDF saved: {expected_filename}")
                    except Exception as e:
                        logger.warning(f"    ⚠️  Could not rename PDF: {e}")
                        result["pdf_path"]       = str(downloaded.resolve())
                        result["pdf_downloaded"] = True
                else:
                    logger.warning("    ⚠️  PDF download timed out — saving metadata anyway")
            else:
                logger.warning("    ⚠️  No PDF link found — saving metadata anyway")
        except Exception as e:
            logger.warning(f"    ⚠️  PDF error: {e} — saving metadata anyway")

    return result


# ─────────────────────────────────────────────────────────────
#  NEXT PAGE
# ─────────────────────────────────────────────────────────────

def get_next_page_url(driver) -> Optional[str]:
    try:
        btn = driver.find_element(
            By.CSS_SELECTOR,
            "a[title='Go to next page'], .pager-next a, li.next a, a[rel='next']"
        )
        return btn.get_attribute("href")
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
#  SAVE TO SQLITE
# ─────────────────────────────────────────────────────────────

def _save(notice: dict, page_data: dict, tender_id: str) -> dict:
    org_id = upsert_organisation(
        name="Unknown",
        organisation_type=None,
        country=None,
    )
    return upsert_tender({
        "source_portal":           PORTAL,
        "tender_id":               tender_id,
        "title":                   notice["title"],
        "notice_text":             None,
        "description":             page_data["description"],
        "country":                 None,
        "notice_type":             None,
        "language":                "English",
        "publication_date":        notice["date_iso"],
        "deadline_date":           page_data["deadline_iso"],
        "deadline_time":           None,
        "budget":                  None,
        "currency":                None,
        "project_id":              None,
        "procurement_group":       None,
        "procurement_method_code": None,
        "procurement_method_name": None,
        "sector":                  None,
        "keywords":                page_data["keywords"],
        "summary":                 None,
        "status_id":               4,
        "organisation_id":         org_id,
        "source_url":              notice["url"],
        "pdf_path":                page_data["pdf_path"],
    })


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run(state: dict) -> None:

    last_url = state["last_url"]

    logger.info("=" * 65)
    logger.info("AFDB Daily Scraper — Stage 1 Ingestion")
    logger.info(f"Today       : {TODAY_ISO}")
    logger.info(f"Cursor      : {last_url or 'none — first run'}")
    logger.info("=" * 65)

    driver              = build_driver()
    current_listing_url = LISTING_URL
    stop                = False
    newest_url_this_run = None   # first new notice saved this run = new cursor

    stats = {
        "saved":       0,
        "expired":     0,
        "pdf_failed":  0,
        "save_errors": 0,
    }

    try:
        while current_listing_url and not stop:

            if not safe_get(driver, current_listing_url):
                logger.error("Browser navigation failed — stopping")
                break

            time.sleep(15)  # wait for JS to render

            notices = scrape_listing(driver)
            logger.info(f"  {len(notices)} notices on this page")

            if not notices:
                logger.warning("  ⚠️  No notices found — check selectors")
                break

            for notice in notices:

                logger.info(f"\n  📌 {notice['title'][:80]}")
                logger.info(f"     Published: {notice['date_iso']} | URL: {notice['url']}")

                tender_id = _url_to_tender_id(notice["url"])

                # ── CURSOR CHECK ──────────────────────────────────────────
                # Stop as soon as we hit a notice already in the DB —
                # everything below was already scraped
                if get_tender_by_ref(PORTAL, tender_id):
                    logger.info("  ⏹  Already in DB — cursor reached, stopping")
                    stop = True
                    break

                # ── YEAR FILTER ───────────────────────────────────────────
                if notice["pub_date"] and not _is_2026(notice["pub_date"]):
                    logger.info(f"  🛑 Publication {notice['date_iso']} before 2026 — stopping")
                    stop = True
                    break

                # ── SCRAPE NOTICE PAGE ────────────────────────────────────
                page_data = scrape_notice_page(driver, notice["url"], tender_id)

                # ── DEADLINE EXPIRY FILTER ────────────────────────────────
                if page_data["deadline_iso"]:
                    dl = _parse_date(page_data["deadline_iso"])
                    if dl and dl < TODAY:
                        logger.info(f"  ⏭  Deadline passed ({page_data['deadline_iso']}) — skipping")
                        stats["expired"] += 1
                        safe_get(driver, current_listing_url)
                        time.sleep(random.uniform(2, 4))
                        continue

                if not page_data["pdf_downloaded"]:
                    stats["pdf_failed"] += 1

                # ── SAVE ──────────────────────────────────────────────────
                try:
                    saved = _save(notice, page_data, tender_id)

                    # Track first new notice saved = new cursor
                    if newest_url_this_run is None:
                        newest_url_this_run = notice["url"]

                    logger.info(
                        f"  ✅  {(saved['title'] or '')[:55]:<55} "
                        f"| deadline:{saved['deadline_date'] or 'in PDF'} "
                        f"| pdf:{'✓' if page_data['pdf_downloaded'] else '✗'} "
                        f"| kw:{'✓' if page_data['keywords'] else '✗'}"
                    )
                    stats["saved"] += 1

                except Exception as e:
                    logger.error(f"  ❌  Save failed: {e} | url={notice['url']}")
                    stats["save_errors"] += 1

                # Return to listing page before next notice
                if not safe_get(driver, current_listing_url):
                    stop = True
                    break
                time.sleep(random.uniform(3, 6))

            if not stop:
                safe_get(driver, current_listing_url)
                time.sleep(random.uniform(2, 4))
                next_url = get_next_page_url(driver)
                if next_url:
                    logger.info(f"\n➡️  Next page: {next_url}")
                    current_listing_url = next_url
                else:
                    logger.info("\n✅ No more pages")
                    break

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrupted — saving checkpoint...")

    finally:
        # Save new cursor — newest notice saved this run
        # If nothing new was saved, keep the old cursor unchanged
        if newest_url_this_run:
            save_state(newest_url_this_run)
            logger.info(f"New cursor → {newest_url_this_run}")
        else:
            logger.info("No new notices saved — cursor unchanged")

        logger.info(f"\n{'=' * 65}")
        logger.info("AFDB DAILY SCRAPE COMPLETE")
        logger.info(f"  Saved       : {stats['saved']}")
        logger.info(f"  Expired     : {stats['expired']}")
        logger.info(f"  PDF failed  : {stats['pdf_failed']}")
        logger.info(f"  Save errors : {stats['save_errors']}")
        logger.info(f"  PDFs in     : {DOWNLOAD_DIR}")
        logger.info(f"{'=' * 65}")
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AFDB Daily Scraper")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear checkpoint — next run fetches from scratch"
    )
    args = parser.parse_args()
    init_db()
    state = reset_state() if args.reset else get_state()
    run(state)