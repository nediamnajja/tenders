"""
enricher/pdf_extractor.py
=========================
Reads AfDB PDFs from disk, extracts clean text,
and saves it into tenders.notice_text.

Reads from  : tenders  (source_portal = 'afdb', pdf_path not null, notice_text null/empty)
Writes to   : tenders  (notice_text)
Skips       : any tender that already has notice_text populated

Run:
    python enricher/pdf_extractor.py               # all unprocessed afdb PDFs
    python enricher/pdf_extractor.py --dry-run     # print extracted text, no DB writes
    python enricher/pdf_extractor.py --limit 10    # process first 10 only
    python enricher/pdf_extractor.py --diagnose    # detailed per-page diagnostics, no DB writes
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional

# ── Project root on sys.path ─────────────────────────────────────────────────
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import pdfplumber
from sqlalchemy import select

from db import get_session
from models import Tender

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

PORTAL = "afdb"

# Minimum chars for the whole document to be considered useful
MIN_TEXT_LENGTH = 100

# Minimum chars per page to consider it a real text page (not image-only)
MIN_PAGE_TEXT_LENGTH = 20


# ─────────────────────────────────────────────────────────────────────────────
#  WORD-LEVEL PAGE EXTRACTION  (core fix)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_page_text(page, page_num: int) -> str:
    """
    Extract text from a single page using word-level bounding boxes.

    Why word-level instead of extract_text()?
    ------------------------------------------
    pdfplumber's extract_text() assembles characters in PDF content-stream order,
    which can be completely different from reading order. It also silently stops
    mid-word when character bounding boxes overlap — a common issue in AfDB PDFs
    that use custom fonts or complex column layouts.

    extract_words() gives us individual word objects with (x0, top, text) coords
    so we can reconstruct lines in true top-to-bottom, left-to-right order.

    Falls back to extract_text() if extract_words() returns nothing.
    """
    # ── Attempt 1: word-level extraction ─────────────────────────────────────
    try:
        words = page.extract_words(
            x_tolerance=3,        # max horizontal gap (pts) to still merge into one word
            y_tolerance=3,        # max vertical gap (pts) to still consider same line
            keep_blank_chars=False,
            use_text_flow=True,   # respects natural reading order where possible
        )

        if words:
            log.debug("    Page %d: %d words found via extract_words()", page_num, len(words))

            # Group words into lines by their top coordinate.
            # Round to nearest 2pt bucket so words on the same visual line
            # (with slightly different tops due to font metrics) are grouped together.
            lines: dict[int, list[dict]] = {}
            for word in words:
                line_key = round(word["top"] / 2) * 2
                lines.setdefault(line_key, []).append(word)

            # Sort lines top-to-bottom, words left-to-right within each line
            result_lines = []
            for _, line_words in sorted(lines.items()):
                line_words.sort(key=lambda w: w["x0"])
                result_lines.append(" ".join(w["text"] for w in line_words))

            text = "\n".join(result_lines)
            if len(text.strip()) >= MIN_PAGE_TEXT_LENGTH:
                return text

            log.debug(
                "    Page %d: extract_words() produced only %d chars, trying fallback",
                page_num, len(text.strip()),
            )

    except Exception as e:
        log.warning(
            "    Page %d: extract_words() failed (%s) — trying extract_text()",
            page_num, e,
        )

    # ── Attempt 2: standard extract_text() as fallback ───────────────────────
    try:
        raw = page.extract_text(x_tolerance=3, y_tolerance=3)
        if raw:
            log.debug(
                "    Page %d: extract_text() fallback produced %d chars",
                page_num, len(raw.strip()),
            )
            return raw
    except Exception as e:
        log.warning("    Page %d: extract_text() fallback also failed: %s", page_num, e)

    return ""


# ─────────────────────────────────────────────────────────────────────────────
#  DIAGNOSTIC MODE
# ─────────────────────────────────────────────────────────────────────────────

def diagnose_pdf(pdf_path: str) -> None:
    """
    Run a detailed per-page diagnostic on a single PDF.
    Prints page count, chars extracted per page, word count, image count,
    and flags likely scanned/image-only pages.
    Does NOT write anything to the DB.
    """
    path = Path(pdf_path)

    log.info("=" * 70)
    log.info("DIAGNOSING: %s", pdf_path)
    log.info("=" * 70)

    if not path.exists():
        log.error("  File not found: %s", pdf_path)
        return
    if path.stat().st_size == 0:
        log.error("  File is empty (0 bytes)")
        return

    log.info("  File size   : %.1f KB", path.stat().st_size / 1024)

    try:
        with pdfplumber.open(str(path)) as pdf:
            total_pages = len(pdf.pages)
            log.info("  Total pages : %d", total_pages)

            if total_pages == 0:
                log.error("  PDF reports 0 pages — likely corrupted")
                return

            total_chars      = 0
            image_only_pages = []
            failed_pages     = []

            log.info("-" * 70)
            log.info(
                "  %-6s %-10s %-10s %-10s %s",
                "Page", "Words", "Chars", "Images", "Status",
            )
            log.info("-" * 70)

            for page_num, page in enumerate(pdf.pages, start=1):
                # Count embedded images
                try:
                    n_images = len(page.images)
                except Exception:
                    n_images = -1

                # Word count (raw, before grouping)
                try:
                    words   = page.extract_words(x_tolerance=3, y_tolerance=3)
                    n_words = len(words)
                except Exception:
                    words   = []
                    n_words = -1

                # Full page text via our robust extractor
                try:
                    text       = _extract_page_text(page, page_num)
                    char_count = len(text.strip())
                except Exception as e:
                    text       = ""
                    char_count = 0
                    failed_pages.append(page_num)
                    log.warning("    Page %d failed entirely: %s", page_num, e)

                total_chars += char_count

                if char_count == 0:
                    status = "⚠ NO TEXT (image-only or blank)"
                    image_only_pages.append(page_num)
                elif char_count < MIN_PAGE_TEXT_LENGTH:
                    status = "⚠ VERY SHORT"
                else:
                    status = "✓ OK"

                log.info(
                    "  %-6d %-10s %-10d %-10s %s",
                    page_num,
                    str(n_words) if n_words >= 0 else "?",
                    char_count,
                    str(n_images) if n_images >= 0 else "?",
                    status,
                )

                # Show a short preview per page so you can spot garbled text
                if char_count >= MIN_PAGE_TEXT_LENGTH:
                    preview = text.strip()[:150].replace("\n", " ")
                    log.info("         Preview: %s…", preview)

            log.info("-" * 70)
            log.info("  TOTAL chars extracted : %d", total_chars)
            log.info("  Image-only pages      : %s", image_only_pages or "none")
            log.info("  Failed pages          : %s", failed_pages     or "none")
            log.info("")

            # Overall verdict
            if total_chars == 0:
                log.warning("  VERDICT: No text at all — fully scanned/image-based PDF.")
                log.warning("           OCR (pytesseract / easyocr) required.")
            elif image_only_pages:
                pct = len(image_only_pages) / total_pages * 100
                log.warning(
                    "  VERDICT: %.0f%% of pages (%d/%d) had no text — mixed PDF.",
                    pct, len(image_only_pages), total_pages,
                )
                log.warning("           OCR fallback recommended for full coverage.")
            elif total_chars < MIN_TEXT_LENGTH:
                log.warning(
                    "  VERDICT: Total text (%d chars) below MIN_TEXT_LENGTH (%d).",
                    total_chars, MIN_TEXT_LENGTH,
                )
            else:
                log.info("  VERDICT: PDF looks fine — extraction should work correctly.")

            log.info("=" * 70)

    except Exception as e:
        log.error("  Failed to open PDF: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
#  PDF TEXT EXTRACTION  (main)
# ─────────────────────────────────────────────────────────────────────────────

def extract_text_from_pdf(pdf_path: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extract and clean text from a PDF file using word-level extraction.

    Returns:
        (clean_text, error_message)
        On success : (text, None)
        On failure : (None, error_string)
    """
    path = Path(pdf_path)

    # ── Basic file checks ─────────────────────────────────────────────────────
    if not path.exists():
        return None, f"File not found: {pdf_path}"
    if not path.is_file():
        return None, f"Path is not a file: {pdf_path}"
    if path.suffix.lower() != ".pdf":
        return None, f"Not a PDF file: {pdf_path}"
    if path.stat().st_size == 0:
        return None, f"Empty file: {pdf_path}"

    # ── Extract ───────────────────────────────────────────────────────────────
    try:
        pages_text:       list[str] = []
        image_only_pages: list[int] = []
        failed_pages:     list[int] = []

        with pdfplumber.open(str(path)) as pdf:
            if len(pdf.pages) == 0:
                return None, "PDF has no pages"

            total_pages = len(pdf.pages)

            for page_num, page in enumerate(pdf.pages, start=1):
                try:
                    text = _extract_page_text(page, page_num)

                    if not text or len(text.strip()) < MIN_PAGE_TEXT_LENGTH:
                        log.warning(
                            "  Page %d/%d: no/minimal text (%d chars) — likely image-only",
                            page_num, total_pages,
                            len(text.strip()) if text else 0,
                        )
                        image_only_pages.append(page_num)
                        continue

                    log.info(
                        "  Page %d/%d: %d chars extracted",
                        page_num, total_pages, len(text.strip()),
                    )
                    pages_text.append(text)

                except Exception as page_err:
                    log.warning(
                        "  Page %d/%d extraction failed: %s",
                        page_num, total_pages, page_err,
                    )
                    failed_pages.append(page_num)
                    continue

        # ── Report ────────────────────────────────────────────────────────────
        if image_only_pages:
            log.warning(
                "  %d/%d pages had no extractable text (image-only): pages %s",
                len(image_only_pages), total_pages, image_only_pages,
            )
        if failed_pages:
            log.warning(
                "  %d/%d pages failed during extraction: pages %s",
                len(failed_pages), total_pages, failed_pages,
            )

        if not pages_text:
            return None, (
                f"No text extracted from any of {total_pages} pages "
                f"(image-only: {image_only_pages}, failed: {failed_pages})"
            )

        log.info(
            "  Extracted text from %d/%d pages  |  image-only: %s  |  failed: %s",
            len(pages_text), total_pages,
            image_only_pages or "none",
            failed_pages     or "none",
        )

        # ── Clean ─────────────────────────────────────────────────────────────
        full_text = "\n".join(pages_text)
        clean     = _clean_pdf_text(full_text)

        if len(clean) < MIN_TEXT_LENGTH:
            return None, (
                f"Extracted text too short ({len(clean)} chars) — "
                f"likely a scanned PDF or near-empty document"
            )

        return clean, None

    except pdfplumber.pdfminer.pdfparser.PDFSyntaxError as e:
        return None, f"Corrupted or invalid PDF: {e}"
    except Exception as e:
        return None, f"Unexpected error reading PDF: {e}"


def _clean_pdf_text(raw: str) -> str:
    """
    Clean raw PDF-extracted text:
    - Normalize line endings
    - Remove form-feed / page-break characters
    - Strip and collapse whitespace per line
    - Collapse more than 2 consecutive blank lines into 2
    """
    text = raw.replace("\r\n", "\n").replace("\r", "\n").replace("\f", "\n")

    lines = []
    for line in text.split("\n"):
        line = re.sub(r"[ \t]+", " ", line).strip()
        lines.append(line)

    cleaned_lines: list[str] = []
    blank_count = 0
    for line in lines:
        if line == "":
            blank_count += 1
            if blank_count <= 2:
                cleaned_lines.append(line)
        else:
            blank_count = 0
            cleaned_lines.append(line)

    return "\n".join(cleaned_lines).strip()


# ─────────────────────────────────────────────────────────────────────────────
#  DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_unprocessed_tenders(session, limit: Optional[int] = None) -> list[Tender]:
    """Return AfDB tenders that have a pdf_path but no notice_text populated yet."""
    stmt = (
        select(Tender)
        .where(Tender.source_portal == PORTAL)
        .where(Tender.pdf_path.isnot(None))
        .where(Tender.pdf_path != "")
        .where(
            (Tender.notice_text.is_(None)) | (Tender.notice_text == "")
        )
        .order_by(Tender.id)
    )

    if limit:
        stmt = stmt.limit(limit)

    return session.execute(stmt).scalars().all()


def _save_notice_text(session, tender: Tender, clean_text: str) -> None:
    """Write extracted text directly into tenders.notice_text."""
    tender.notice_text = clean_text
    session.flush()


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN RUN
# ─────────────────────────────────────────────────────────────────────────────

def run_pdf_extraction(
    dry_run:  bool          = False,
    limit:    Optional[int] = None,
    diagnose: bool          = False,
) -> None:
    """
    Main entry point.
    Finds unprocessed AfDB tenders → extracts PDF text → saves to tenders.notice_text.
    """
    counters = dict(total=0, success=0, failed=0, skipped=0)

    with get_session() as session:
        tenders    = _get_unprocessed_tenders(session, limit=limit)
        tender_ids = [(t.id, t.tender_id, t.pdf_path) for t in tenders]

    counters["total"] = len(tender_ids)
    log.info("Found %d unprocessed AfDB tenders with PDFs", counters["total"])

    if counters["total"] == 0:
        log.info("Nothing to do — all AfDB tenders already have notice_text populated.")
        return

    # ── Diagnose mode: per-file report, no DB writes ──────────────────────────
    if diagnose:
        log.info("Running in DIAGNOSE mode — no DB writes will occur.")
        for db_id, notice_id, pdf_path in tender_ids:
            log.info("")
            log.info("Tender id=%s  notice=%s", db_id, notice_id)
            diagnose_pdf(pdf_path)
        return

    # ── Normal extraction ─────────────────────────────────────────────────────
    for db_id, notice_id, pdf_path in tender_ids:
        log.info("Processing tender id=%s  notice=%s", db_id, notice_id)
        log.info("  PDF: %s", pdf_path)

        clean_text, error = extract_text_from_pdf(pdf_path)

        if error:
            log.warning("  ✗ Extraction failed: %s", error)
            counters["failed"] += 1
            continue

        if dry_run:
            preview = clean_text[:400].replace("\n", " ")
            log.info("  [DRY-RUN] Extracted %d chars — preview: %s…", len(clean_text), preview)
            counters["success"] += 1
            continue

        try:
            with get_session() as session:
                tender_obj = session.get(Tender, db_id)
                if not tender_obj:
                    log.warning("  Tender id=%s disappeared from DB, skipping", db_id)
                    counters["skipped"] += 1
                    continue
                _save_notice_text(session, tender_obj, clean_text)
            log.info("  ✓ Saved %d chars to tenders.notice_text", len(clean_text))
            counters["success"] += 1

        except Exception as db_err:
            log.error("  DB write failed for id=%s: %s", db_id, db_err, exc_info=True)
            counters["failed"] += 1

    log.info(
        "PDF extraction done — total=%d  success=%d  failed=%d  skipped=%d%s",
        counters["total"], counters["success"],
        counters["failed"], counters["skipped"],
        "  [DRY-RUN — nothing written]" if dry_run else "",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract text from AfDB PDFs into tenders.notice_text."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and print text preview without writing to the DB.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N tenders (useful for testing).",
    )
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help=(
            "Run detailed per-page diagnostics (word count, char count, image count). "
            "No DB writes — use this to understand why text is incomplete."
        ),
    )
    args = parser.parse_args()
    run_pdf_extraction(dry_run=args.dry_run, limit=args.limit, diagnose=args.diagnose)