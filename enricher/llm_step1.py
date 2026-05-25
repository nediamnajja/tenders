"""
enricher/llm_step1_prefilter.py
================================
STEP 1 — Clean notice texts and store in normalized_tenders.notice_text_clean.

Reads tenders.notice_text for all portals.
Strips boilerplate, HTML, contacts, bank details.
Writes cleaned text to normalized_tenders.notice_text_clean.

Deadline source:
  - WorldBank / UNGM / UNDP → tenders.deadline_date
  - AfDB                    → enriched_tenders.deadline_datetime

Skips:
  - Contract award notices and GPNs
  - Tenders with no notice_text or text too short
  - Tenders whose deadline has passed
  - Tenders already cleaned (notice_text_clean IS NOT NULL) unless --all

Run:
    python enricher/llm_step1_prefilter.py
    python enricher/llm_step1_prefilter.py --portal afdb
    python enricher/llm_step1_prefilter.py --portal worldbank
    python enricher/llm_step1_prefilter.py --limit 50
    python enricher/llm_step1_prefilter.py --all        # re-clean already cleaned
    python enricher/llm_step1_prefilter.py --dry-run    # print only, no DB writes
"""

import argparse
import logging
import os
import re
import sys
from datetime import date, datetime, timezone

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

ALL_PORTALS = ["afdb", "worldbank", "ungm", "undp"]

# =============================================================================
# NOTICE TYPE DETECTION — skip these entirely
# =============================================================================

_SKIP_NOTICE_PATTERNS = re.compile(
    r"""
      attribution\s+de\s+march
    | avis\s+d.attribution
    | publication\s+de\s+l.attribution
    | contract\s+award\s+notice
    | award\s+of\s+contract
    | notification\s+d.attribution
    | note\s+d.information
    | general\s+procurement\s+notice
    | avis\s+g[eé]n[eé]ral\s+de\s+passation
    | \bgpn\b
    | procurement\s+data\s+entry
    | donn[eé]es\s+des\s+acquisitions
    | \bpde\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


def should_skip_notice(title: str, notice_text: str) -> tuple[bool, str]:
    combined = (title or "") + " " + (notice_text or "")[:500]
    if _SKIP_NOTICE_PATTERNS.search(combined):
        return True, "contract_award_or_gpn"
    return False, ""


# =============================================================================
# BOILERPLATE DETECTION
# =============================================================================

_BOILERPLATE_STARTS = [
    "the government of", "le gouvernement de", "a reçu un financement",
    "a reçu un don", "a reçu un prêt", "has received financing from",
    "has received a grant", "has received a loan", "has received fund",
    "intends to apply part", "entend affecter une partie",
    "a l'intention d'utiliser", "elle a l'intention d'utiliser",
    "and intends to apply",
    "the african development bank", "avenue joseph anoma",
    "african development bank group", "banque africaine de développement",
    "the world bank", "international bank for reconstruction",
    "toward the cost of", "procurement regulations for ipf borrowers",
    "world bank procurement regulations",
    "règlement de passation des marchés pour les emprunteurs",
    "règlement de passation de marchés pour les emprunteurs",
    "eligibility criteria, establishment", "les critères d'éligibilité",
    "the selection procedure shall be", "la procédure de sélection sera",
    "la procédure de sélection seront", "which is available on the bank",
    "available on the bank's website", "disponible sur le site web de la banque",
    "this is available on", "http://www.afdb.org", "https://www.afdb.org",
    "the bidding document in", "le dossier d'appel d'offres en",
    "le dossier complet de demande de cotations", "le document d'appel d'offres",
    "documents will be sent", "nonrefundable fee", "frais non remboursable",
    "paiement non remboursable", "western union", "money gram",
    "attn:", "attention:", "street address:", "floor/", "floor /",
    "zip/postal", "private bag", "p.o. box", "p o box",
    "tel:", "telephone:", "tél:", "téléphone:", "e-mail:", "email:",
    "a l'attention de", "à l'attention de", "bureau :", "bureau:",
    "adresse :", "adresse:", "pays :", "pays:",
    "l'adresse à laquelle", "l'adresse referée",
    "the address referred to above", "the address(es) referred to above",
    "kindly send your inquiries", "for further information",
    "des informations supplémentaires peuvent être obtenues",
    "further information can be obtained", "pour de plus amples",
    "pour plus d'informations", "les consultants intéressés peuvent obtenir",
    "interested consultants may obtain", "interested firms may obtain",
    "interested eligible bidders may obtain",
    "interested eligible proposers may obtain",
    "les soumissionnaires intéressés et éligibles peuvent",
    "bank name:", "account name:", "account number:",
    "nom de la banque", "intitulé du compte", "numéro du compte",
    "code swift", "iban:", "nib:", "swift", "bank:", "account no:", "beneficiary:",
    "nb:", "n.b.:", "attention is drawn to the procurement",
    "veuillez noter que le règlement de passation",
    "il est porté à l'attention", "l'attention est attirée sur",
    "l'attention des", "tout consultant qui souhaite",
    "the bank reserves the right", "la banque se réserve",
    "sexual exploitation", "sea/sh", "beneficial ownership", "beneficial_ownership",
    "signed:", "le coordonnateur", "la coordonnatrice", "coordonnatrice",
    "director general", "le directeur", "nothing follows", "---",
    "nom attributaire", "nom de l'attributaire", "montant du contrat",
    "date de démarrage", "durée d'exécution", "nombre total de soumissionnaires",
    "synthèse de l'objet",
]

_WB_PARA1_RE = re.compile(
    r"^1\.\s+(?:the republic|the government|le gouvernement|la r[eé]publique|the federal|the kingdom)",
    re.IGNORECASE,
)

_AFDB_BACKGROUND_SKIP_RE = re.compile(
    r"^(?:the african development bank|the afdb|the bank).{0,120}"
    r"(?:hereby invites|invites qualified|intends to appoint|acting as)",
    re.IGNORECASE,
)


def _is_boilerplate(line: str) -> bool:
    lower = line.strip().lower()
    return bool(lower) and any(lower.startswith(bp) for bp in _BOILERPLATE_STARTS)


# =============================================================================
# HTML + TEXT CLEANING
# =============================================================================

def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    replacements = {
        "&nbsp;": " ", "&ensp;": " ", "&emsp;": " ", "&amp;": "&",
        "&lt;": "<", "&gt;": ">", "&bull;": "•", "&ndash;": "–",
        "&mdash;": "—", "&laquo;": "«", "&raquo;": "»", "&hellip;": "…",
        "&rsquo;": "'", "&lsquo;": "'", "&ldquo;": '"', "&rdquo;": '"',
        "&apos;": "'", "&quot;": '"',
        "&ccedil;": "ç", "&Ccedil;": "Ç", "&eacute;": "é", "&Eacute;": "É",
        "&egrave;": "è", "&Egrave;": "È", "&ecirc;": "ê", "&Ecirc;": "Ê",
        "&euml;": "ë", "&agrave;": "à", "&Agrave;": "À", "&acirc;": "â",
        "&Acirc;": "Â", "&auml;": "ä", "&icirc;": "î", "&Icirc;": "Î",
        "&iuml;": "ï", "&ocirc;": "ô", "&Ocirc;": "Ô", "&oelig;": "œ",
        "&OElig;": "Œ", "&ucirc;": "û", "&Ucirc;": "Û", "&uacute;": "ú",
        "&ugrave;": "ù", "&uuml;": "ü", "&ntilde;": "ñ", "&Ntilde;": "Ñ",
        "&atilde;": "ã", "&otilde;": "õ", "&aacute;": "á", "&oacute;": "ó",
        "&iacute;": "í", "&scedil;": "ş", "&Scedil;": "Ş",
        "&deg;": "°", "&euro;": "€", "&pound;": "£",
    }
    for entity, char in replacements.items():
        text = text.replace(entity, char)
    text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
    text = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), text)
    return text


def _strip_contacts(text: str) -> str:
    text = re.sub(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b", "", text)
    text = re.sub(r"(?<!\d)(\+?\d[\d\s\.\-\(\)\/]{6,20}\d)(?!\d)", "", text)
    return text


def _strip_bank_details(text: str) -> str:
    text = re.sub(r"\bIBAN\b.*",          "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSWIFT\b.*",         "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bAccount\s*No\.?.*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bNIB\b.*",           "", text, flags=re.IGNORECASE)
    return text


def prepare_for_llm(notice_text: str, portal: str = "afdb", max_words: int = 700) -> str:
    """
    Clean a raw notice_text for LLM input.
    Returns empty string if text is too short after cleaning.
    """
    if not notice_text or not notice_text.strip():
        return ""

    text = _strip_html(notice_text)
    text = _strip_contacts(text)
    text = _strip_bank_details(text)

    lines  = text.splitlines()
    kept   = []
    skip_n = 0

    for line in lines:
        stripped = line.strip()

        if skip_n > 0:
            skip_n -= 1
            continue

        if not stripped:
            kept.append("")
            continue

        if _is_boilerplate(stripped):
            skip_n = 1
            continue

        if portal == "worldbank" and _WB_PARA1_RE.match(stripped):
            skip_n = 3
            continue

        if portal == "afdb" and _AFDB_BACKGROUND_SKIP_RE.match(stripped):
            continue

        kept.append(stripped)

    result = "\n".join(kept)
    result = re.sub(r" {2,}", " ", result)
    result = re.sub(r"\n{3,}", "\n\n", result)
    result = result.strip()

    words = result.split()
    if len(words) > max_words:
        result = " ".join(words[:max_words]) + "\n[truncated at 700 words]"

    # Safety fallback — if we stripped too much, return minimally cleaned text
    if len(result.split()) < 50:
        fallback = _strip_html(notice_text)
        fallback = _strip_contacts(fallback)
        fallback = _strip_bank_details(fallback)
        fallback = re.sub(r"\n{3,}", "\n\n", fallback).strip()
        words = fallback.split()
        if len(words) > max_words:
            fallback = " ".join(words[:max_words]) + "\n[truncated at 700 words]"
        return fallback

    return result


# =============================================================================
# DB QUERY
# =============================================================================

def fetch_eligible_tenders(
    session,
    portals:  list[str],
    limit:    int | None = None,
    re_run:   bool = False,
) -> list[dict]:
    """
    Fetch tenders that:
      - Have notice_text (not null, length > 200)
      - Have a future deadline
        * WorldBank/UNGM/UNDP → tenders.deadline_date
        * AfDB                → enriched_tenders.deadline_datetime
      - Have not yet been cleaned (unless re_run=True)

    Uses LEFT JOINs so AfDB tenders (no normalized_tenders row yet,
    deadline in enriched_tenders) are not silently excluded.
    """
    from sqlalchemy import text

    today    = date.today().isoformat()
    today_ts = datetime.now(timezone.utc).isoformat()

    portal_list = ", ".join(f"'{p}'" for p in portals)

    # Not yet cleaned = no normalized row at all, OR row exists but text is NULL
    already_cleaned_filter = "" if re_run else \
        "AND (nt.id IS NULL OR nt.notice_text_clean IS NULL)"

    sql = f"""
        SELECT
            t.id            AS tender_db_id,
            t.title,
            t.notice_text,
            t.source_portal,
            t.deadline_date,
            nt.id           AS normalized_id,
            et.deadline_datetime
        FROM tenders t
        -- LEFT JOIN so AfDB tenders with no normalized row are still returned
        LEFT JOIN normalized_tenders nt ON nt.tender_id = t.id
        -- LEFT JOIN to get deadline_datetime for AfDB
        LEFT JOIN enriched_tenders   et ON et.tender_id = t.id
        WHERE
            t.source_portal IN ({portal_list})
            AND t.notice_text IS NOT NULL
            AND LENGTH(t.notice_text) > 200
            AND (
                -- WorldBank / UNGM / UNDP use tenders.deadline_date
                t.deadline_date >= :today
                OR
                -- AfDB uses enriched_tenders.deadline_datetime
                et.deadline_datetime >= :today_ts
            )
            {already_cleaned_filter}
        ORDER BY COALESCE(t.deadline_date::text, et.deadline_datetime::date::text) ASC
        {f'LIMIT {limit}' if limit else ''}
    """

    rows = session.execute(
        text(sql),
        {"today": today, "today_ts": today_ts},
    ).mappings().all()
    return [dict(row) for row in rows]


# =============================================================================
# DB WRITE
# =============================================================================

def write_cleaned_text(session, tender_db_id: int, cleaned_text: str) -> None:
    """
    Upsert cleaned text into normalized_tenders.notice_text_clean.
    Creates the row if it doesn't exist (AfDB case).
    Updates notice_text_clean if the row already exists (WorldBank case).
    """
    from sqlalchemy import text

    session.execute(
        text("""
            INSERT INTO normalized_tenders (tender_id, notice_text_clean)
            VALUES (:tid, :txt)
            ON CONFLICT (tender_id)
            DO UPDATE SET notice_text_clean = :txt
        """),
        {"tid": tender_db_id, "txt": cleaned_text},
    )


# =============================================================================
# MAIN
# =============================================================================

def run(
    portals:  list[str] | None = None,
    limit:    int | None = None,
    re_run:   bool = False,
    dry_run:  bool = False,
) -> int:
    try:
        from db import SessionLocal
    except ImportError as e:
        log.error("Import error: %s — run from project root", e)
        return 0

    portals   = portals or ALL_PORTALS
    session   = SessionLocal()
    total     = 0
    written   = 0
    skipped   = 0
    too_short = 0

    try:
        log.info("Fetching eligible tenders (portals=%s, re_run=%s)", portals, re_run)
        notices = fetch_eligible_tenders(
            session, portals=portals, limit=limit, re_run=re_run,
        )
        total = len(notices)
        log.info("Found %d eligible tenders with notice_text", total)

        for i, notice in enumerate(notices, 1):
            tender_id = notice["tender_db_id"]
            title     = (notice["title"] or "")[:70]
            portal    = notice["source_portal"]

            # Skip contract awards and GPNs
            skip, reason = should_skip_notice(
                notice.get("title", ""),
                notice.get("notice_text", ""),
            )
            if skip:
                skipped += 1
                log.info(
                    "  [%d/%d] SKIP (%s) id=%s  %s",
                    i, total, reason, tender_id, title,
                )
                continue

            # Clean the text
            cleaned = prepare_for_llm(notice["notice_text"], portal=portal)

            if not cleaned or len(cleaned.split()) < 50:
                too_short += 1
                log.info(
                    "  [%d/%d] TOO SHORT after cleaning id=%s  %s",
                    i, total, tender_id, title,
                )
                continue

            words_before = len(notice["notice_text"].split())
            words_after  = len(cleaned.split())
            reduction    = round((1 - words_after / max(words_before, 1)) * 100)

            log.info(
                "  [%d/%d] id=%-6s  portal=%-10s  %d→%d words (%d%% cut)  %s",
                i, total, tender_id, portal,
                words_before, words_after, reduction, title,
            )

            if dry_run:
                written += 1
                continue

            # Upsert cleaned text — creates normalized_tenders row if missing
            write_cleaned_text(session, tender_id, cleaned)
            written += 1

            # Commit every 50 rows to avoid large transactions
            if written % 50 == 0:
                session.commit()
                log.info("  ... committed %d rows so far", written)

        if not dry_run:
            session.commit()

    except Exception as e:
        session.rollback()
        log.error("Fatal error: %s", e, exc_info=True)
    finally:
        session.close()

    print(f"\n{'='*60}")
    print(f"  STEP 1 SUMMARY {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"  Eligible tenders found   : {total}")
    print(f"  Skipped (award/GPN)      : {skipped}")
    print(f"  Skipped (too short)      : {too_short}")
    print(f"  Written to DB            : {written}")
    print(f"  Portals                  : {', '.join(portals)}")
    print(f"{'='*60}\n")

    return written


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Step 1: Clean notice texts and store in normalized_tenders.notice_text_clean."
    )
    parser.add_argument(
        "--portal", choices=ALL_PORTALS, default=None,
        help="Process one portal only (default: all four).",
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Process at most N tenders.",
    )
    parser.add_argument(
        "--all", action="store_true", dest="re_run",
        help="Re-clean tenders that already have notice_text_clean.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run cleaning and print results without writing to DB.",
    )
    args = parser.parse_args()

    portals = [args.portal] if args.portal else ALL_PORTALS

    run(
        portals = portals,
        limit   = args.limit,
        re_run  = args.re_run,
        dry_run = args.dry_run,
    )