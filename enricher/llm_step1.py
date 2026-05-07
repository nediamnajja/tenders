
# # """
# # enricher/llm_step1_prefilter.py
# # ================================
# # STEP 1 ONLY — Select + pre-filter notice texts from the tenders table.

# # Selects 5 AfDB + 5 World Bank notices with variety (consulting, works,
# # goods, French, English). Strips boilerplate, HTML, contacts, bank details.
# # Prints cleaned output so you can verify BEFORE sending anything to the LLM.

# # No API calls. No DB writes. Read-only.

# # Run:
# #     python enricher/llm_step1_prefilter.py
# #     python enricher/llm_step1_prefilter.py --save          # save cleaned texts to JSON
# #     python enricher/llm_step1_prefilter.py --portal afdb   # only AfDB
# #     python enricher/llm_step1_prefilter.py --portal worldbank
# # """

# # import argparse
# # import json
# # import logging
# # import os
# # import re
# # import sys

# # ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# # if ROOT_DIR not in sys.path:
# #     sys.path.insert(0, ROOT_DIR)

# # logging.basicConfig(
# #     level=logging.INFO,
# #     format="%(asctime)s [%(levelname)s] %(message)s",
# # )
# # log = logging.getLogger(__name__)


# # # ─────────────────────────────────────────────────────────────────────────────
# # #  SELECTION QUERIES
# # #  Pick by variety: mix of procurement groups, languages, notice lengths.
# # #  Exclude nulls and very short texts (under 100 chars = useless).
# # # ─────────────────────────────────────────────────────────────────────────────

# # # For each portal we run separate queries targeting different procurement types.
# # # LIMIT 2 on most, LIMIT 1 on others to get to 5 total with variety.

# # AFDB_QUERIES = [
# #     # 2 consulting English
# #     """
# #     SELECT id, title, notice_text, source_portal
# #     FROM tenders
# #     WHERE source_portal = 'afdb'
# #       AND notice_text IS NOT NULL
# #       AND LENGTH(notice_text) > 100
# #       AND (
# #         LOWER(notice_text) LIKE '%consulting%'
# #         OR LOWER(notice_text) LIKE '%consultant%'
# #         OR LOWER(notice_text) LIKE '%expression of interest%'
# #       )
# #       AND (
# #         LOWER(notice_text) LIKE '%english%'
# #         OR LOWER(title) LIKE '%consultant%'
# #         OR LOWER(title) LIKE '%advisory%'
# #         OR LOWER(title) LIKE '%expert%'
# #       )
# #     ORDER BY RANDOM()
# #     LIMIT 2
# #     """,
# #     # 1 consulting French
# #     """
# #     SELECT id, title, notice_text, source_portal
# #     FROM tenders
# #     WHERE source_portal = 'afdb'
# #       AND notice_text IS NOT NULL
# #       AND LENGTH(notice_text) > 100
# #       AND (
# #         LOWER(notice_text) LIKE '%consultant%'
# #         OR LOWER(notice_text) LIKE '%manifestation%'
# #         OR LOWER(notice_text) LIKE '%expression d%intérêt%'
# #       )
# #       AND (
# #         LOWER(notice_text) LIKE '%le %'
# #         OR LOWER(notice_text) LIKE '%les %'
# #         OR LOWER(notice_text) LIKE '%du %'
# #       )
# #     ORDER BY RANDOM()
# #     LIMIT 1
# #     """,
# #     # 1 works (construction, rehabilitation, travaux)
# #     """
# #     SELECT id, title, notice_text, source_portal
# #     FROM tenders
# #     WHERE source_portal = 'afdb'
# #       AND notice_text IS NOT NULL
# #       AND LENGTH(notice_text) > 100
# #       AND (
# #         LOWER(notice_text) LIKE '%travaux%'
# #         OR LOWER(notice_text) LIKE '%construction%'
# #         OR LOWER(notice_text) LIKE '%rehabilitation%'
# #         OR LOWER(notice_text) LIKE '%réhabilitation%'
# #         OR LOWER(title) LIKE '%works%'
# #         OR LOWER(title) LIKE '%construction%'
# #       )
# #     ORDER BY RANDOM()
# #     LIMIT 1
# #     """,
# #     # 1 goods or mixed
# #     """
# #     SELECT id, title, notice_text, source_portal
# #     FROM tenders
# #     WHERE source_portal = 'afdb'
# #       AND notice_text IS NOT NULL
# #       AND LENGTH(notice_text) > 100
# #       AND (
# #         LOWER(notice_text) LIKE '%supply%'
# #         OR LOWER(notice_text) LIKE '%fourniture%'
# #         OR LOWER(notice_text) LIKE '%equipment%'
# #         OR LOWER(notice_text) LIKE '%goods%'
# #         OR LOWER(title) LIKE '%supply%'
# #         OR LOWER(title) LIKE '%procurement of%'
# #       )
# #     ORDER BY RANDOM()
# #     LIMIT 1
# #     """,
# # ]

# # WORLDBANK_QUERIES = [
# #     # 2 consulting / individual
# #     """
# #     SELECT id, title, notice_text, source_portal
# #     FROM tenders
# #     WHERE source_portal = 'worldbank'
# #       AND notice_text IS NOT NULL
# #       AND LENGTH(notice_text) > 100
# #       AND (
# #         LOWER(notice_text) LIKE '%consulting%'
# #         OR LOWER(notice_text) LIKE '%individual consultant%'
# #         OR LOWER(notice_text) LIKE '%expression of interest%'
# #         OR LOWER(notice_text) LIKE '%terms of reference%'
# #       )
# #     ORDER BY RANDOM()
# #     LIMIT 2
# #     """,
# #     # 1 works
# #     """
# #     SELECT id, title, notice_text, source_portal
# #     FROM tenders
# #     WHERE source_portal = 'worldbank'
# #       AND notice_text IS NOT NULL
# #       AND LENGTH(notice_text) > 100
# #       AND (
# #         LOWER(notice_text) LIKE '%civil works%'
# #         OR LOWER(notice_text) LIKE '%construction%'
# #         OR LOWER(notice_text) LIKE '%rehabilitation%'
# #         OR LOWER(title) LIKE '%works%'
# #         OR LOWER(title) LIKE '%road%'
# #         OR LOWER(title) LIKE '%bridge%'
# #       )
# #     ORDER BY RANDOM()
# #     LIMIT 1
# #     """,
# #     # 1 goods
# #     """
# #     SELECT id, title, notice_text, source_portal
# #     FROM tenders
# #     WHERE source_portal = 'worldbank'
# #       AND notice_text IS NOT NULL
# #       AND LENGTH(notice_text) > 100
# #       AND (
# #         LOWER(notice_text) LIKE '%supply%'
# #         OR LOWER(notice_text) LIKE '%procurement of%'
# #         OR LOWER(notice_text) LIKE '%request for bids%'
# #         OR LOWER(notice_text) LIKE '%goods%'
# #         OR LOWER(title) LIKE '%supply%'
# #         OR LOWER(title) LIKE '%equipment%'
# #       )
# #     ORDER BY RANDOM()
# #     LIMIT 1
# #     """,
# #     # 1 French or non-English
# #     """
# #     SELECT id, title, notice_text, source_portal
# #     FROM tenders
# #     WHERE source_portal = 'worldbank'
# #       AND notice_text IS NOT NULL
# #       AND LENGTH(notice_text) > 100
# #       AND (
# #         LOWER(notice_text) LIKE '%le %'
# #         OR LOWER(notice_text) LIKE '%les offres%'
# #         OR LOWER(notice_text) LIKE '%date limite%'
# #         OR LOWER(notice_text) LIKE '%appel d%offres%'
# #       )
# #     ORDER BY RANDOM()
# #     LIMIT 1
# #     """,
# # ]


# # # ─────────────────────────────────────────────────────────────────────────────
# # #  PRE-FILTER
# # # ─────────────────────────────────────────────────────────────────────────────

# # # ─────────────────────────────────────────────────────────────────────────────
# # #  NOTICE TYPE DETECTION — skip LLM enrichment for these types
# # # ─────────────────────────────────────────────────────────────────────────────

# # # Titles or first lines matching these patterns = contract award or GPN.
# # # These notice types should NOT be sent to the LLM.
# # _SKIP_NOTICE_PATTERNS = re.compile(
# #     r"""
# #     # Contract award notices
# #       attribution\s+de\s+march
# #     | avis\s+d.attribution
# #     | publication\s+de\s+l.attribution
# #     | contract\s+award\s+notice
# #     | award\s+of\s+contract
# #     | notification\s+d.attribution
# #     | note\s+d.information
# #     # General Procurement Notices
# #     | general\s+procurement\s+notice
# #     | avis\s+g[eé]n[eé]ral\s+de\s+passation
# #     | gpn
# #     # Procurement Data Entry forms
# #     | procurement\s+data\s+entry
# #     | donn[eé]es\s+des\s+acquisitions
# #     | pde
# #     # Corrigendum — keep but flag
# #     # (we don't skip, LLM extracts notice_type = corrigendum)
# #     """,
# #     re.IGNORECASE | re.VERBOSE,
# # )

# # def should_skip_notice(title: str, notice_text: str) -> tuple[bool, str]:
# #     """
# #     Returns (should_skip, reason).
# #     True for contract award notices and GPNs — no LLM enrichment needed.
# #     """
# #     combined = (title or "") + " " + (notice_text or "")[:500]
# #     if _SKIP_NOTICE_PATTERNS.search(combined):
# #         return True, "contract_award_or_gpn"
# #     # Also skip if notice text is extremely short after stripping
# #     return False, ""


# # # ─────────────────────────────────────────────────────────────────────────────
# # #  BOILERPLATE DETECTION
# # # ─────────────────────────────────────────────────────────────────────────────

# # # Lines/paragraphs starting with these phrases are pure boilerplate — drop them.
# # # Covers AfDB, World Bank, GCF notices in English, French, Portuguese, Spanish.
# # _BOILERPLATE_STARTS = [
# #     # ── Financing intro (already scraped) ─────────────────────────────────────
# #     "the government of",
# #     "le gouvernement de",
# #     "a reçu un financement",
# #     "a reçu un don",
# #     "a reçu un prêt",
# #     "has received financing from",
# #     "has received a grant",
# #     "has received a loan",
# #     "has received fund",
# #     "intends to apply part",
# #     "entend affecter une partie",
# #     "a l'intention d'utiliser",
# #     "elle a l'intention d'utiliser",
# #     "and intends to apply",
# #     # ── AfDB standard header ───────────────────────────────────────────────────
# #     "the african development bank",
# #     "avenue joseph anoma",
# #     "african development bank group",
# #     "banque africaine de développement",
# #     # ── World Bank standard boilerplate ───────────────────────────────────────
# #     "the world bank",
# #     "international bank for reconstruction",
# #     "toward the cost of",
# #     "procurement regulations for ipf borrowers",
# #     "world bank procurement regulations",
# #     "règlement de passation des marchés pour les emprunteurs",
# #     "règlement de passation de marchés pour les emprunteurs",
# #     # ── Eligibility / procedure boilerplate ───────────────────────────────────
# #     "eligibility criteria, establishment",
# #     "les critères d'éligibilité",
# #     "the selection procedure shall be",
# #     "la procédure de sélection sera",
# #     "la procédure de sélection seront",
# #     "which is available on the bank",
# #     "available on the bank's website",
# #     "disponible sur le site web de la banque",
# #     "this is available on",
# #     "http://www.afdb.org",
# #     "https://www.afdb.org",
# #     # ── Document purchase / access ────────────────────────────────────────────
# #     "the bidding document in",
# #     "le dossier d'appel d'offres en",
# #     "le dossier complet de demande de cotations",
# #     "le document d'appel d'offres",
# #     "documents will be sent",
# #     "nonrefundable fee",
# #     "frais non remboursable",
# #     "paiement non remboursable",
# #     "western union",
# #     "money gram",
# #     # ── Contact / address block ───────────────────────────────────────────────
# #     "attn:",
# #     "attention:",
# #     "street address:",
# #     "floor/",
# #     "floor /",
# #     "zip/postal",
# #     "private bag",
# #     "p.o. box",
# #     "p o box",
# #     "tel:",
# #     "telephone:",
# #     "tél:",
# #     "téléphone:",
# #     "e-mail:",
# #     "email:",
# #     "a l'attention de",
# #     "à l'attention de",
# #     "bureau :",
# #     "bureau:",
# #     "adresse :",
# #     "adresse:",
# #     "pays :",
# #     "pays:",
# #     "l'adresse à laquelle",
# #     "l'adresse referée",
# #     "the address referred to above",
# #     "the address(es) referred to above",
# #     "kindly send your inquiries",
# #     "for further information",
# #     "des informations supplémentaires peuvent être obtenues",
# #     "further information can be obtained",
# #     "pour de plus amples",
# #     "pour plus d'informations",
# #     "les consultants intéressés peuvent obtenir",
# #     "interested consultants may obtain",
# #     "interested firms may obtain",
# #     "interested eligible bidders may obtain",
# #     "interested eligible proposers may obtain",
# #     "les soumissionnaires intéressés et éligibles peuvent",
# #     # ── Submission mechanics (deadline already scraped) ───────────────────────
# #     "expressions of interest must be",
# #     "expressions d'intérêt doivent être",
# #     "les manifestations d'intérêt doivent",
# #     "must be delivered to",
# #     "must be submitted",
# #     "doivent être déposées",
# #     "doivent être remises",
# #     "doivent être soumises",
# #     "bids must be delivered",
# #     "bids must be accompanied",
# #     "all bids must",
# #     "toutes les offres doivent",
# #     "les offres doivent être remises",
# #     "les offres devront être",
# #     "les offres devront être soumises",
# #     "les cotations dûment signées",
# #     # ── Bid opening (procedural) ──────────────────────────────────────────────
# #     # ── Bank / payment details ────────────────────────────────────────────────
# #     "bank name:",
# #     "account name:",
# #     "account number:",
# #     "nom de la banque",
# #     "intitulé du compte",
# #     "numéro du compte",
# #     "code swift",
# #     "iban:",
# #     "nib:",
# #     "swift",
# #     "bank:",
# #     "account no:",
# #     "beneficiary:",
# #     # ── Legal / compliance closing ────────────────────────────────────────────
# #     "nb:",
# #     "n.b.:",
# #     "attention is drawn to the procurement",
# #     "veuillez noter que le règlement de passation",
# #     "il est porté à l'attention",
# #     "l'attention est attirée sur",
# #     "l'attention des",
# #     "tout consultant qui souhaite",
# #     "the bank reserves the right",
# #     "la banque se réserve",
# #     "sexual exploitation",
# #     "sea/sh",
# #     "beneficial ownership",
# #     "beneficial_ownership",
# #     # ── Signature / sign-off blocks ───────────────────────────────────────────
# #     "signed:",
# #     "le coordonnateur",
# #     "la coordonnatrice",
# #     "coordonnatrice",
# #     "coordinator",
# #     "director general",
# #     "le directeur",
# #     "nothing follows",
# #     "---",
# #     # ── Contract award specific ───────────────────────────────────────────────
# #     "nom attributaire",
# #     "nom de l'attributaire",
# #     "montant du contrat",
# #     "date de démarrage",
# #     "durée d'exécution",
# #     "nombre total de soumissionnaires",
# #     "pour chaque soumissionnaire",
# #     "notes techniques",
# #     "prix évalués",
# #     "notes finales",
# #     "classement",
# #     "nationalité",
# #     "date d'approbation par la banque",
# #     "méthode de sélection",
# #     "date de publication de l'ami",
# #     "synthèse de l'objet",
# # ]

# # # World Bank numbered paragraph 1 is always financing boilerplate — skip it
# # _WB_PARA1_RE = re.compile(
# #     r"^1\.\s+(?:the republic|the government|le gouvernement|la r[eé]publique|the federal|the kingdom)",
# #     re.IGNORECASE,
# # )

# # # AfDB background section opening sentences to skip
# # _AFDB_BACKGROUND_SKIP_RE = re.compile(
# #     r"^(?:the african development bank|the afdb|the bank).{0,120}"
# #     r"(?:hereby invites|invites qualified|intends to appoint|acting as)",
# #     re.IGNORECASE,
# # )




# # def _strip_html(text: str) -> str:
# #     text = re.sub(r"<[^>]+>", " ", text)
# #     replacements = {
# #         # spaces
# #         "&nbsp;": " ", "&ensp;": " ", "&emsp;": " ", "&thinsp;": " ",
# #         # symbols
# #         "&amp;": "&", "&lt;": "<", "&gt;": ">",
# #         "&bull;": "•", "&middot;": "·", "&ndash;": "–", "&mdash;": "—",
# #         "&laquo;": "«", "&raquo;": "»", "&hellip;": "…",
# #         # quotes
# #         "&rsquo;": "'", "&lsquo;": "'", "&ldquo;": '"', "&rdquo;": '"',
# #         "&apos;": "'", "&quot;": '"',
# #         # French chars
# #         "&ccedil;": "ç", "&Ccedil;": "Ç",
# #         "&eacute;": "é", "&Eacute;": "É",
# #         "&egrave;": "è", "&Egrave;": "È",
# #         "&ecirc;": "ê",  "&Ecirc;": "Ê",
# #         "&euml;": "ë",
# #         "&agrave;": "à", "&Agrave;": "À",
# #         "&acirc;": "â",  "&Acirc;": "Â",
# #         "&auml;": "ä",
# #         "&icirc;": "î",  "&Icirc;": "Î",
# #         "&iuml;": "ï",
# #         "&ocirc;": "ô",  "&Ocirc;": "Ô",
# #         "&oelig;": "œ",  "&OElig;": "Œ",
# #         "&ucirc;": "û",  "&Ucirc;": "Û",
# #         "&uacute;": "ú", "&ugrave;": "ù", "&uuml;": "ü",
# #         "&ntilde;": "ñ", "&Ntilde;": "Ñ",
# #         # Portuguese / Spanish
# #         "&atilde;": "ã", "&otilde;": "õ",
# #         "&aacute;": "á", "&oacute;": "ó", "&iacute;": "í",
# #         # Turkish
# #         "&scedil;": "ş", "&Scedil;": "Ş",
# #         "&gbreve;": "ğ", "&Gbreve;": "Ğ",
# #         "&idot;": "İ",
# #         # Other
# #         "&deg;": "°", "&sup2;": "²", "&sup3;": "³",
# #         "&frac12;": "½", "&frac14;": "¼",
# #         "&times;": "×", "&divide;": "÷",
# #         "&euro;": "€", "&pound;": "£",
# #     }
# #     for entity, char in replacements.items():
# #         text = text.replace(entity, char)
# #     # catch any remaining numeric entities
# #     text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
# #     text = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), text)
# #     return text


# # def _strip_contacts(text: str) -> str:
# #     text = re.sub(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b", "", text)
# #     text = re.sub(r"(?<!\d)(\+?\d[\d\s\.\-\(\)\/]{6,20}\d)(?!\d)", "", text)
# #     return text


# # def _strip_bank_details(text: str) -> str:
# #     text = re.sub(r"\bIBAN\b.*", "", text, flags=re.IGNORECASE)
# #     text = re.sub(r"\bSWIFT\b.*", "", text, flags=re.IGNORECASE)
# #     text = re.sub(r"\bAccount\s*No\.?.*", "", text, flags=re.IGNORECASE)
# #     text = re.sub(r"\bNIB\b.*", "", text, flags=re.IGNORECASE)
# #     return text


# # def _is_boilerplate(line: str) -> bool:
# #     lower = line.strip().lower()
# #     if not lower:
# #         return False
# #     return any(lower.startswith(bp) for bp in _BOILERPLATE_STARTS)


# # def prepare_for_llm(notice_text: str, portal: str = "afdb", max_words: int = 700) -> str:
# #     """
# #     Clean a raw notice_text for LLM input.

# #     portal: 'afdb' or 'worldbank' — slight differences in boilerplate structure.
# #     max_words: hard cap on output length.
# #     """
# #     if not notice_text or not notice_text.strip():
# #         return ""

# #     # 1. Strip HTML tags and entities
# #     text = _strip_html(notice_text)

# #     # 2. Strip contacts and bank details (already in DB)
# #     text = _strip_contacts(text)
# #     text = _strip_bank_details(text)

# #     # 3. Split into lines and filter
# #     lines = text.splitlines()
# #     kept = []
# #     skip_next_n = 0  # used to skip N lines after a boilerplate trigger

# #     for i, line in enumerate(lines):
# #         stripped = line.strip()

# #         # Skip blank continuation of a boilerplate block
# #         if skip_next_n > 0:
# #             skip_next_n -= 1
# #             continue

# #         if not stripped:
# #             kept.append("")
# #             continue

# #         # Drop boilerplate lines
# #         if _is_boilerplate(stripped):
# #             skip_next_n = 1  # also drop the next line (usually empty or continuation)
# #             continue

# #         # World Bank: drop paragraph 1 (always financing intro boilerplate)
# #         if portal == "worldbank" and _WB_PARA1_RE.match(stripped):
# #             skip_next_n = 3
# #             continue

# #         # AfDB: drop the standard "AfDB hereby invites..." opening sentence
# #         if portal == "afdb" and _AFDB_BACKGROUND_SKIP_RE.match(stripped):
# #             continue

# #         kept.append(stripped)

# #     # 4. Rejoin and collapse whitespace
# #     result = "\n".join(kept)
# #     result = re.sub(r" {2,}", " ", result)
# #     result = re.sub(r"\n{3,}", "\n\n", result)
# #     result = result.strip()

# #     # 5. Hard cap at max_words
# #     words = result.split()
# #     if len(words) > max_words:
# #         result = " ".join(words[:max_words]) + "\n[truncated at 700 words]"

# #     # 6. Safety fallback — if we stripped too much, return minimally cleaned text
# #     if len(result.split()) < 50:
# #         fallback = _strip_html(notice_text)
# #         fallback = _strip_contacts(fallback)
# #         fallback = _strip_bank_details(fallback)
# #         fallback = re.sub(r"\n{3,}", "\n\n", fallback).strip()
# #         words = fallback.split()
# #         if len(words) > max_words:
# #             fallback = " ".join(words[:max_words]) + "\n[truncated at 700 words]"
# #         return fallback

# #     return result


# # # ─────────────────────────────────────────────────────────────────────────────
# # #  DB SELECTION
# # # ─────────────────────────────────────────────────────────────────────────────

# # def fetch_notices(portal: str, session) -> list[dict]:
# #     """
# #     Run variety queries for a portal, deduplicate by id,
# #     return up to 5 notices.
# #     """
# #     from sqlalchemy import text

# #     queries = AFDB_QUERIES if portal == "afdb" else WORLDBANK_QUERIES
# #     seen_ids = set()
# #     results = []

# #     for sql in queries:
# #         rows = session.execute(text(sql)).mappings().all()
# #         for row in rows:
# #             if row["id"] not in seen_ids:
# #                 seen_ids.add(row["id"])
# #                 results.append(dict(row))
# #             if len(results) >= 5:
# #                 break
# #         if len(results) >= 5:
# #             break

# #     # If variety queries didn't reach 5, fill with any remaining notices
# #     if len(results) < 5:
# #         fallback_sql = f"""
# #             SELECT id, title, notice_text, source_portal
# #             FROM tenders
# #             WHERE source_portal = '{portal}'
# #               AND notice_text IS NOT NULL
# #               AND LENGTH(notice_text) > 100
# #               AND id NOT IN ({','.join(str(r['id']) for r in results) or '0'})
# #             ORDER BY RANDOM()
# #             LIMIT {5 - len(results)}
# #         """
# #         rows = session.execute(text(fallback_sql)).mappings().all()
# #         for row in rows:
# #             results.append(dict(row))

# #     return results[:5]


# # # ─────────────────────────────────────────────────────────────────────────────
# # #  MAIN
# # # ─────────────────────────────────────────────────────────────────────────────

# # def run(portal_filter: str | None = None, save: bool = False) -> None:
# #     try:
# #         from db import SessionLocal
# #     except ImportError as e:
# #         log.error("Import error: %s — run from project root", e)
# #         return

# #     portals = ["afdb", "worldbank"]
# #     if portal_filter:
# #         portals = [portal_filter]

# #     all_results = []

# #     session = SessionLocal()
# #     try:
# #         for portal in portals:
# #             log.info("Fetching 5 notices from portal: %s", portal)
# #             notices = fetch_notices(portal, session)
# #             log.info("  Got %d notices", len(notices))

# #             for notice in notices:
# #                 # Skip contract award notices and GPNs
# #                 skip, skip_reason = should_skip_notice(
# #                     notice.get("title", ""),
# #                     notice.get("notice_text", ""),
# #                 )
# #                 if skip:
# #                     log.info("  Skipping id=%s (%s): %s", notice["id"], skip_reason, (notice.get("title") or "")[:60])
# #                     continue

# #                 cleaned = prepare_for_llm(
# #                     notice["notice_text"],
# #                     portal=portal,
# #                 )
# #                 word_count_before = len(notice["notice_text"].split())
# #                 word_count_after  = len(cleaned.split())
# #                 reduction_pct     = round((1 - word_count_after / max(word_count_before, 1)) * 100)

# #                 result = {
# #                     "id":           notice["id"],
# #                     "portal":       portal,
# #                     "title":        (notice["title"] or "")[:100],
# #                     "words_before": word_count_before,
# #                     "words_after":  word_count_after,
# #                     "reduction_pct": reduction_pct,
# #                     "raw_text":     notice["notice_text"],
# #                     "cleaned_text": cleaned,
# #                 }
# #                 all_results.append(result)

# #                 # Print to console
# #                 print(f"\n{'═'*70}")
# #                 print(f"  Portal  : {portal.upper()}")
# #                 print(f"  ID      : {notice['id']}")
# #                 print(f"  Title   : {result['title']}")
# #                 print(f"  Words   : {word_count_before} → {word_count_after} ({reduction_pct}% reduction)")
# #                 print(f"{'─'*70}")
# #                 print("  ▼ RAW NOTICE TEXT (full):")
# #                 print(f"{'─'*70}")
# #                 print(notice["notice_text"])
# #                 print(f"{'─'*70}")
# #                 print("  ▼ CLEANED TEXT (after filter):")
# #                 print(f"{'─'*70}")
# #                 print(cleaned)

# #     finally:
# #         session.close()

# #     # Summary
# #     print(f"\n{'═'*70}")
# #     print("  SUMMARY")
# #     print(f"{'─'*70}")
# #     for r in all_results:
# #         print(
# #             f"  [{r['portal'].upper():<10}] id={r['id']:<6} "
# #             f"{r['words_before']:>5} → {r['words_after']:>4} words "
# #             f"({r['reduction_pct']}% cut)  {r['title'][:50]}"
# #         )
# #     print(f"{'═'*70}\n")

# #     # Always save to CSV
# #     import csv
# #     csv_path = "enricher/llm_step1_output.csv"
# #     with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
# #         writer = csv.DictWriter(f, fieldnames=["id", "portal", "title", "words_before", "words_after", "reduction_pct", "raw_text", "cleaned_text"])
# #         writer.writeheader()
# #         for r in all_results:
# #             writer.writerow(r)
# #     log.info("Saved CSV -> %s", csv_path)

# #     # Save to JSON for step 2
# #     if save:
# #         out_path = "enricher/llm_step1_output.json"
# #         with open(out_path, "w", encoding="utf-8") as f:
# #             json.dump(all_results, f, ensure_ascii=False, indent=2)
# #         log.info("Saved cleaned texts to %s", out_path)
# #         print(f"  Saved → {out_path}")
# #         print("  (Pass this file to step 2 so it doesn't re-query the DB)\n")


# # # ─────────────────────────────────────────────────────────────────────────────
# # #  CLI
# # # ─────────────────────────────────────────────────────────────────────────────

# # if __name__ == "__main__":
# #     parser = argparse.ArgumentParser(
# #         description="Step 1: select + pre-filter 5 AfDB + 5 WorldBank notices."
# #     )
# #     parser.add_argument(
# #         "--portal", choices=["afdb", "worldbank"], default=None,
# #         help="Run for one portal only (default: both).",
# #     )
# #     parser.add_argument(
# #         "--save", action="store_true",
# #         help="Save cleaned texts to enricher/llm_step1_output.json for step 2.",
# #     )
# #     args = parser.parse_args()
# #     run(portal_filter=args.portal, save=args.save)



# """
# enricher/llm_step1_prefilter.py
# ================================
# STEP 1 — Select + pre-filter ALL eligible notice texts from the tenders table.

# Selects all tenders from afdb and worldbank portals that:
#   - Have a deadline in the future (deadline_date > today)
#   - Have notice_text (not null, not too short)
#   - Have not yet been LLM-enriched (enriched_tenders.llm_scope_summary IS NULL)
#   - Are not contract awards or GPNs

# Strips boilerplate, HTML, contacts, bank details.
# Saves cleaned texts to enricher/llm_step1_output.csv for step 2.

# No API calls. No DB writes. Read-only.

# Run:
#     python enricher/llm_step1_prefilter.py
#     python enricher/llm_step1_prefilter.py --portal afdb
#     python enricher/llm_step1_prefilter.py --portal worldbank
#     python enricher/llm_step1_prefilter.py --limit 20
#     python enricher/llm_step1_prefilter.py --all       # re-process already enriched
# """

# import argparse
# import csv
# import logging
# import os
# import re
# import sys
# from datetime import date

# ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# if ROOT_DIR not in sys.path:
#     sys.path.insert(0, ROOT_DIR)

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
# )
# log = logging.getLogger(__name__)

# SUPPORTED_PORTALS = ["afdb", "worldbank"]

# # ─────────────────────────────────────────────────────────────────────────────
# #  NOTICE TYPE DETECTION
# # ─────────────────────────────────────────────────────────────────────────────

# _SKIP_NOTICE_PATTERNS = re.compile(
#     r"""
#       attribution\s+de\s+march
#     | avis\s+d.attribution
#     | publication\s+de\s+l.attribution
#     | contract\s+award\s+notice
#     | award\s+of\s+contract
#     | notification\s+d.attribution
#     | note\s+d.information
#     | general\s+procurement\s+notice
#     | avis\s+g[eé]n[eé]ral\s+de\s+passation
#     | \bgpn\b
#     | procurement\s+data\s+entry
#     | donn[eé]es\s+des\s+acquisitions
#     | \bpde\b
#     """,
#     re.IGNORECASE | re.VERBOSE,
# )


# def should_skip_notice(title: str, notice_text: str) -> tuple[bool, str]:
#     combined = (title or "") + " " + (notice_text or "")[:500]
#     if _SKIP_NOTICE_PATTERNS.search(combined):
#         return True, "contract_award_or_gpn"
#     return False, ""


# # ─────────────────────────────────────────────────────────────────────────────
# #  BOILERPLATE DETECTION
# # ─────────────────────────────────────────────────────────────────────────────

# _BOILERPLATE_STARTS = [
#     "the government of", "le gouvernement de", "a reçu un financement",
#     "a reçu un don", "a reçu un prêt", "has received financing from",
#     "has received a grant", "has received a loan", "has received fund",
#     "intends to apply part", "entend affecter une partie",
#     "a l'intention d'utiliser", "elle a l'intention d'utiliser",
#     "and intends to apply",
#     "the african development bank", "avenue joseph anoma",
#     "african development bank group", "banque africaine de développement",
#     "the world bank", "international bank for reconstruction",
#     "toward the cost of", "procurement regulations for ipf borrowers",
#     "world bank procurement regulations",
#     "règlement de passation des marchés pour les emprunteurs",
#     "règlement de passation de marchés pour les emprunteurs",
#     "eligibility criteria, establishment", "les critères d'éligibilité",
#     "the selection procedure shall be", "la procédure de sélection sera",
#     "la procédure de sélection seront", "which is available on the bank",
#     "available on the bank's website", "disponible sur le site web de la banque",
#     "this is available on", "http://www.afdb.org", "https://www.afdb.org",
#     "the bidding document in", "le dossier d'appel d'offres en",
#     "le dossier complet de demande de cotations", "le document d'appel d'offres",
#     "documents will be sent", "nonrefundable fee", "frais non remboursable",
#     "paiement non remboursable", "western union", "money gram",
#     "attn:", "attention:", "street address:", "floor/", "floor /",
#     "zip/postal", "private bag", "p.o. box", "p o box",
#     "tel:", "telephone:", "tél:", "téléphone:", "e-mail:", "email:",
#     "a l'attention de", "à l'attention de", "bureau :", "bureau:",
#     "adresse :", "adresse:", "pays :", "pays:",
#     "l'adresse à laquelle", "l'adresse referée",
#     "the address referred to above", "the address(es) referred to above",
#     "kindly send your inquiries", "for further information",
#     "des informations supplémentaires peuvent être obtenues",
#     "further information can be obtained", "pour de plus amples",
#     "pour plus d'informations", "les consultants intéressés peuvent obtenir",
#     "interested consultants may obtain", "interested firms may obtain",
#     "interested eligible bidders may obtain",
#     "interested eligible proposers may obtain",
#     "les soumissionnaires intéressés et éligibles peuvent",
#     "bank name:", "account name:", "account number:",
#     "nom de la banque", "intitulé du compte", "numéro du compte",
#     "code swift", "iban:", "nib:", "swift", "bank:", "account no:", "beneficiary:",
#     "nb:", "n.b.:", "attention is drawn to the procurement",
#     "veuillez noter que le règlement de passation",
#     "il est porté à l'attention", "l'attention est attirée sur",
#     "l'attention des", "tout consultant qui souhaite",
#     "the bank reserves the right", "la banque se réserve",
#     "sexual exploitation", "sea/sh", "beneficial ownership", "beneficial_ownership",
#     "signed:", "le coordonnateur", "la coordonnatrice", "coordonnatrice",
#     "director general", "le directeur", "nothing follows", "---",
#     "nom attributaire", "nom de l'attributaire", "montant du contrat",
#     "date de démarrage", "durée d'exécution", "nombre total de soumissionnaires",
#     "synthèse de l'objet",
# ]

# _WB_PARA1_RE = re.compile(
#     r"^1\.\s+(?:the republic|the government|le gouvernement|la r[eé]publique|the federal|the kingdom)",
#     re.IGNORECASE,
# )

# _AFDB_BACKGROUND_SKIP_RE = re.compile(
#     r"^(?:the african development bank|the afdb|the bank).{0,120}"
#     r"(?:hereby invites|invites qualified|intends to appoint|acting as)",
#     re.IGNORECASE,
# )


# def _is_boilerplate(line: str) -> bool:
#     lower = line.strip().lower()
#     return bool(lower) and any(lower.startswith(bp) for bp in _BOILERPLATE_STARTS)


# # ─────────────────────────────────────────────────────────────────────────────
# #  HTML + TEXT CLEANING
# # ─────────────────────────────────────────────────────────────────────────────

# def _strip_html(text: str) -> str:
#     text = re.sub(r"<[^>]+>", " ", text)
#     replacements = {
#         "&nbsp;": " ", "&ensp;": " ", "&emsp;": " ", "&amp;": "&",
#         "&lt;": "<", "&gt;": ">", "&bull;": "•", "&ndash;": "–",
#         "&mdash;": "—", "&laquo;": "«", "&raquo;": "»", "&hellip;": "…",
#         "&rsquo;": "'", "&lsquo;": "'", "&ldquo;": '"', "&rdquo;": '"',
#         "&apos;": "'", "&quot;": '"',
#         "&ccedil;": "ç", "&Ccedil;": "Ç", "&eacute;": "é", "&Eacute;": "É",
#         "&egrave;": "è", "&Egrave;": "È", "&ecirc;": "ê", "&Ecirc;": "Ê",
#         "&euml;": "ë", "&agrave;": "à", "&Agrave;": "À", "&acirc;": "â",
#         "&Acirc;": "Â", "&auml;": "ä", "&icirc;": "î", "&Icirc;": "Î",
#         "&iuml;": "ï", "&ocirc;": "ô", "&Ocirc;": "Ô", "&oelig;": "œ",
#         "&OElig;": "Œ", "&ucirc;": "û", "&Ucirc;": "Û", "&uacute;": "ú",
#         "&ugrave;": "ù", "&uuml;": "ü", "&ntilde;": "ñ", "&Ntilde;": "Ñ",
#         "&atilde;": "ã", "&otilde;": "õ", "&aacute;": "á", "&oacute;": "ó",
#         "&iacute;": "í", "&scedil;": "ş", "&Scedil;": "Ş",
#         "&deg;": "°", "&euro;": "€", "&pound;": "£",
#     }
#     for entity, char in replacements.items():
#         text = text.replace(entity, char)
#     text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
#     text = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), text)
#     return text


# def _strip_contacts(text: str) -> str:
#     text = re.sub(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b", "", text)
#     text = re.sub(r"(?<!\d)(\+?\d[\d\s\.\-\(\)\/]{6,20}\d)(?!\d)", "", text)
#     return text


# def _strip_bank_details(text: str) -> str:
#     text = re.sub(r"\bIBAN\b.*",          "", text, flags=re.IGNORECASE)
#     text = re.sub(r"\bSWIFT\b.*",         "", text, flags=re.IGNORECASE)
#     text = re.sub(r"\bAccount\s*No\.?.*", "", text, flags=re.IGNORECASE)
#     text = re.sub(r"\bNIB\b.*",           "", text, flags=re.IGNORECASE)
#     return text


# def prepare_for_llm(notice_text: str, portal: str = "afdb", max_words: int = 700) -> str:
#     if not notice_text or not notice_text.strip():
#         return ""

#     text = _strip_html(notice_text)
#     text = _strip_contacts(text)
#     text = _strip_bank_details(text)

#     lines  = text.splitlines()
#     kept   = []
#     skip_n = 0

#     for line in lines:
#         stripped = line.strip()

#         if skip_n > 0:
#             skip_n -= 1
#             continue

#         if not stripped:
#             kept.append("")
#             continue

#         if _is_boilerplate(stripped):
#             skip_n = 1
#             continue

#         if portal == "worldbank" and _WB_PARA1_RE.match(stripped):
#             skip_n = 3
#             continue

#         if portal == "afdb" and _AFDB_BACKGROUND_SKIP_RE.match(stripped):
#             continue

#         kept.append(stripped)

#     result = "\n".join(kept)
#     result = re.sub(r" {2,}", " ", result)
#     result = re.sub(r"\n{3,}", "\n\n", result)
#     result = result.strip()

#     words = result.split()
#     if len(words) > max_words:
#         result = " ".join(words[:max_words]) + "\n[truncated at 700 words]"

#     if len(result.split()) < 50:
#         fallback = _strip_html(notice_text)
#         fallback = _strip_contacts(fallback)
#         fallback = _strip_bank_details(fallback)
#         fallback = re.sub(r"\n{3,}", "\n\n", fallback).strip()
#         words = fallback.split()
#         if len(words) > max_words:
#             fallback = " ".join(words[:max_words]) + "\n[truncated at 700 words]"
#         return fallback

#     return result


# # ─────────────────────────────────────────────────────────────────────────────
# #  DB QUERY
# # ─────────────────────────────────────────────────────────────────────────────

# def fetch_eligible_tenders(
#     session,
#     portal: str | None = None,
#     limit:  int | None = None,
#     re_run: bool = False,
# ) -> list[dict]:
#     from sqlalchemy import text

#     today       = date.today().isoformat()
#     portals     = [portal] if portal else SUPPORTED_PORTALS
#     portal_list = ", ".join(f"'{p}'" for p in portals)

#     already_enriched_filter = "" if re_run else """
#         AND (et.llm_scope_summary IS NULL OR et.id IS NULL)
#     """

#     sql = f"""
#         SELECT
#             t.id,
#             t.title,
#             t.notice_text,
#             t.source_portal,
#             t.deadline_date
#         FROM tenders t
#         LEFT JOIN enriched_tenders et ON et.tender_id = t.id
#         WHERE
#             t.source_portal IN ({portal_list})
#             AND t.notice_text IS NOT NULL
#             AND LENGTH(t.notice_text) > 200
#             AND t.deadline_date >= :today
#             {already_enriched_filter}
#         ORDER BY t.deadline_date ASC
#         {f'LIMIT {limit}' if limit else ''}
#     """

#     rows = session.execute(text(sql), {"today": today}).mappings().all()
#     return [dict(row) for row in rows]


# # ─────────────────────────────────────────────────────────────────────────────
# #  MAIN
# # ─────────────────────────────────────────────────────────────────────────────

# def run(
#     portal:   str | None = None,
#     limit:    int | None = None,
#     re_run:   bool = False,
#     out_path: str = "enricher/llm_step1_output.csv",
# ) -> int:
#     try:
#         from db import SessionLocal
#     except ImportError as e:
#         log.error("Import error: %s — run from project root", e)
#         return 0

#     session = SessionLocal()
#     results = []
#     skipped = 0
#     notices = []

#     try:
#         log.info("Fetching eligible tenders (portal=%s, re_run=%s)", portal or "all", re_run)
#         notices = fetch_eligible_tenders(session, portal=portal, limit=limit, re_run=re_run)
#         log.info("Found %d eligible tenders", len(notices))

#         for i, notice in enumerate(notices, 1):
#             skip, reason = should_skip_notice(
#                 notice.get("title", ""),
#                 notice.get("notice_text", ""),
#             )
#             if skip:
#                 skipped += 1
#                 continue

#             portal_name = notice["source_portal"]
#             cleaned     = prepare_for_llm(notice["notice_text"], portal=portal_name)
#             words_before = len(notice["notice_text"].split())
#             words_after  = len(cleaned.split())
#             reduction    = round((1 - words_after / max(words_before, 1)) * 100)

#             results.append({
#                 "id":            notice["id"],
#                 "portal":        portal_name,
#                 "title":         (notice["title"] or "")[:100],
#                 "deadline":      notice.get("deadline_date", ""),
#                 "words_before":  words_before,
#                 "words_after":   words_after,
#                 "reduction_pct": reduction,
#                 "raw_text":      notice["notice_text"],
#                 "cleaned_text":  cleaned,
#             })

#             if i % 100 == 0:
#                 log.info("  Processed %d / %d ...", i, len(notices))

#     finally:
#         session.close()

#     if results:
#         os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
#         fieldnames = ["id", "portal", "title", "deadline", "words_before",
#                       "words_after", "reduction_pct", "raw_text", "cleaned_text"]
#         with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
#             writer = csv.DictWriter(f, fieldnames=fieldnames)
#             writer.writeheader()
#             writer.writerows(results)
#         log.info("Saved %d cleaned notices -> %s", len(results), out_path)

#     print(f"\n{'='*60}")
#     print(f"  STEP 1 SUMMARY")
#     print(f"{'='*60}")
#     print(f"  Eligible tenders found : {len(notices)}")
#     print(f"  Skipped (award/GPN)    : {skipped}")
#     print(f"  Saved to CSV           : {len(results)}")
#     print(f"  Output                 : {out_path}")
#     print(f"{'='*60}\n")

#     return len(results)


# # ─────────────────────────────────────────────────────────────────────────────
# #  CLI
# # ─────────────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Step 1: Clean all eligible tenders for LLM enrichment."
#     )
#     parser.add_argument("--portal", choices=SUPPORTED_PORTALS, default=None)
#     parser.add_argument("--limit",  type=int, default=None, metavar="N")
#     parser.add_argument("--all",    action="store_true", dest="re_run",
#                         help="Re-process already enriched tenders.")
#     parser.add_argument("--out",    default="enricher/llm_step1_output.csv")
#     args = parser.parse_args()

#     run(portal=args.portal, limit=args.limit, re_run=args.re_run, out_path=args.out)


"""
enricher/llm_step1_prefilter.py
================================
STEP 1 — Clean notice texts and store in normalized_tenders.notice_text_clean.

Reads tenders.notice_text for all portals.
Strips boilerplate, HTML, contacts, bank details.
Writes cleaned text to normalized_tenders.notice_text_clean.

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
from datetime import date

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
      - Have a normalized_tenders row (so we can write notice_text_clean)
      - Have not yet been cleaned (unless re_run=True)
    All portals supported.
    """
    from sqlalchemy import text

    today       = date.today().isoformat()
    portal_list = ", ".join(f"'{p}'" for p in portals)

    already_cleaned_filter = "" if re_run else \
        "AND nt.notice_text_clean IS NULL"

    sql = f"""
        SELECT
            t.id          AS tender_db_id,
            t.title,
            t.notice_text,
            t.source_portal,
            t.deadline_date,
            nt.id         AS normalized_id
        FROM tenders t
        INNER JOIN normalized_tenders nt ON nt.tender_id = t.id
        WHERE
            t.source_portal IN ({portal_list})
            AND t.notice_text IS NOT NULL
            AND LENGTH(t.notice_text) > 200
            AND t.deadline_date >= :today
            {already_cleaned_filter}
        ORDER BY t.deadline_date ASC
        {f'LIMIT {limit}' if limit else ''}
    """

    rows = session.execute(text(sql), {"today": today}).mappings().all()
    return [dict(row) for row in rows]


# =============================================================================
# DB WRITE
# =============================================================================

def write_cleaned_text(session, normalized_id: int, cleaned_text: str) -> None:
    """Write cleaned text to normalized_tenders.notice_text_clean."""
    from sqlalchemy import text
    session.execute(
        text("UPDATE normalized_tenders SET notice_text_clean = :txt WHERE id = :id"),
        {"txt": cleaned_text, "id": normalized_id},
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

    portals = portals or ALL_PORTALS

    session  = SessionLocal()
    total    = 0
    written  = 0
    skipped  = 0
    too_short = 0

    try:
        log.info("Fetching eligible tenders (portals=%s, re_run=%s)", portals, re_run)
        notices = fetch_eligible_tenders(session, portals=portals, limit=limit, re_run=re_run)
        total   = len(notices)
        log.info("Found %d eligible tenders with notice_text", total)

        for i, notice in enumerate(notices, 1):
            tender_id     = notice["tender_db_id"]
            normalized_id = notice["normalized_id"]
            title         = (notice["title"] or "")[:70]
            portal        = notice["source_portal"]

            # Skip contract awards and GPNs
            skip, reason = should_skip_notice(
                notice.get("title", ""),
                notice.get("notice_text", ""),
            )
            if skip:
                skipped += 1
                log.info("  [%d/%d] SKIP (%s) id=%s  %s", i, total, reason, tender_id, title)
                continue

            # Clean the text
            cleaned = prepare_for_llm(notice["notice_text"], portal=portal)

            if not cleaned or len(cleaned.split()) < 50:
                too_short += 1
                log.info("  [%d/%d] TOO SHORT after cleaning id=%s  %s", i, total, tender_id, title)
                continue

            words_before = len(notice["notice_text"].split())
            words_after  = len(cleaned.split())
            reduction    = round((1 - words_after / max(words_before, 1)) * 100)

            log.info(
                "  [%d/%d] id=%-6s  portal=%-10s  %d→%d words (%d%% cut)  %s",
                i, total, tender_id, portal, words_before, words_after, reduction, title,
            )

            if dry_run:
                written += 1
                continue

            # Write to DB
            write_cleaned_text(session, normalized_id, cleaned)
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