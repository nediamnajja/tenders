"""
enricher/stage2b_rule_extraction.py
=====================================
PIPELINE STEP 2 — Rule-based structured extraction from tenders.notice_text.

Reads every EnrichedTender row with status = 'seeded' (written by stage2a),
fetches the raw notice_text from tenders, runs all rule-based extractors,
then fills in only the fields that are still NULL in EnrichedTender.

MERGE RULE:
    If EnrichedTender already has a non-NULL value for a field
    (seeded from NormalizedTender), that value is KEPT.
    Rule-based extraction only fills gaps.
    Exception: days_to_deadline is ALWAYS recomputed (changes daily).

FIELDS EXTRACTED:
    budget, currency, deadline_datetime, days_to_deadline,
    language, contact_email, contact_phone, procurement_group

FIELDS INTENTIONALLY SKIPPED (unreliable from raw text):
    contact_name, organisation_name

DEADLINE ENGINE — anchor-window approach:
    1. Find all keyword anchor positions in the flat text string.
    2. Find all date candidate positions in the flat text string.
    3. For each date, compute distance to the nearest keyword anchor.
       Dates within ±WINDOW chars of a keyword get a high confidence score.
       Dates with no nearby keyword get a low confidence score.
    4. Parse every valid candidate. Keep all that are future dates.
    5. Return the EARLIEST valid future date (not the first by position).
    6. Month-year fallback ("March 2026") if no full date is found.

Run:
    python enricher/stage2b_rule_extraction.py               # all seeded tenders
    python enricher/stage2b_rule_extraction.py --dry-run     # print results, no DB writes
    python enricher/stage2b_rule_extraction.py --limit 10    # process first 10 only
    python enricher/stage2b_rule_extraction.py --test        # run self-test (no DB)
"""

import argparse
import calendar
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from dateutil import parser as dateutil_parser
from dateutil.relativedelta import relativedelta

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

ALLOWED_PROC_GROUPS = {"GOODS", "WORKS", "CONSULTING", "NON-CONSULTING"}
ALLOWED_LANGUAGES   = {"English", "French", "Arabic", "Portuguese", "Spanish"}
DEFAULT_LANGUAGE    = "English"

BUDGET_MIN = 1_000
BUDGET_MAX = 1_000_000_000

NOW                 = datetime.now(timezone.utc)
DEADLINE_MAX_FUTURE = NOW + relativedelta(years=2)

DEFAULT_PORTALS = ["afdb", "worldbank", "undp", "ungm"]

# Maximum character distance between a keyword anchor and a date for
# the date to be considered "anchored" (high confidence).
# 300 chars ~ 3-4 typical lines, handles table-cell layouts.
DEADLINE_WINDOW = 300


# ─────────────────────────────────────────────────────────────────────────────
#  DATACLASS  (no contact_name, no organisation_name)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StructuredExtraction:
    budget:            Optional[float]    = None
    currency:          Optional[str]      = None
    deadline_datetime: Optional[datetime] = None
    days_to_deadline:  Optional[int]      = None
    language:          Optional[str]      = None
    contact_email:     Optional[str]      = None
    contact_phone:     Optional[str]      = None
    procurement_group: Optional[str]      = None
    _warnings:         list               = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _combine_text(*parts: Optional[str]) -> str:
    return "\n".join(p for p in parts if p).strip()


# ─────────────────────────────────────────────────────────────────────────────
#  BUDGET & CURRENCY
# ─────────────────────────────────────────────────────────────────────────────

_CURRENCY_ALIASES: dict[str, str] = {
    # USD
    "USD": "USD", "US$": "USD", "U.S.D": "USD", "DOLLAR": "USD", "DOLLARS": "USD",
    # EUR
    "EUR": "EUR", "€": "EUR", "EURO": "EUR", "EUROS": "EUR",
    # GBP
    "GBP": "GBP", "£": "GBP", "POUND": "GBP", "POUNDS": "GBP", "POUND STERLING": "GBP",
    # XOF — West African CFA franc
    "XOF": "XOF", "FCFA": "XOF", "F CFA": "XOF", "CFA": "XOF",
    "FRANC CFA": "XOF", "FRANCS CFA": "XOF",
    # XAF — Central African CFA franc
    "XAF": "XAF", "CFAF": "XAF", "FRANCS CFAF": "XAF",
    # TND
    "TND": "TND", "DT": "TND", "DINAR": "TND", "DINARS": "TND",
    "DINAR TUNISIEN": "TND",
    # MAD
    "MAD": "MAD", "DH": "MAD", "DIRHAM": "MAD", "DIRHAMS": "MAD",
    # DZD
    "DZD": "DZD", "DA": "DZD", "DINAR ALGERIEN": "DZD", "DINAR ALGÉRIEN": "DZD",
    # NGN
    "NGN": "NGN", "NAIRA": "NGN",
    # KES
    "KES": "KES", "KSH": "KES", "KENYA SHILLING": "KES",
    # ETB
    "ETB": "ETB", "BIRR": "ETB",
    # ZAR
    "ZAR": "ZAR", "RAND": "ZAR",
    # CHF
    "CHF": "CHF", "FRANC SUISSE": "CHF", "SWISS FRANC": "CHF",
    # CAD
    "CAD": "CAD", "C$": "CAD", "CANADIAN DOLLAR": "CAD",
    # JPY
    "JPY": "JPY", "YEN": "JPY",
    # MXN
    "MXN": "MXN",
    # AfDB Unit of Account / SDR
    "UA": "UA", "UC": "UA", "SDR": "SDR",
}

_CCY_PAT = (
    r"(?:"
    + "|".join(re.escape(k) for k in sorted(_CURRENCY_ALIASES, key=len, reverse=True))
    + r")"
)

_BUDGET_PATTERNS = [
    rf"(?P<ccy1>{_CCY_PAT})\s*(?P<amt1>\d[\d,\.\s]*)\s*(?P<mul1>[MmKkBb](?:illi(?:on|ons)?)?)?",
    rf"(?P<amt2>\d[\d,\.\s]*)\s*(?P<mul2>[MmKkBb](?:illi(?:on|ons)?)?)?\s*(?P<ccy2>{_CCY_PAT})",
    rf"montant\s+(?:estim[eé]\s+)?(?:total\s+)?(?:du\s+contrat\s+)?(?:de\s+)?(?P<amt3>\d[\d,\.]*)\s*(?P<mul3>[MmKkBb](?:illi(?:on|ons)?)?)?\s*(?P<ccy3>{_CCY_PAT})",
    rf"(?:estimated|contract|total)\s+(?:contract\s+)?value\s*(?:of\s*)?[:\-]?\s*(?P<ccy4>{_CCY_PAT})\s*(?P<amt4>\d[\d,\.]*)\s*(?P<mul4>[MmKkBb](?:illi(?:on|ons)?)?)?",
    rf"(?:estimated|contract|total)\s+(?:contract\s+)?value\s*(?:of\s*)?[:\-]?\s*(?P<amt5>\d[\d,\.]*)\s*(?P<mul5>[MmKkBb](?:illi(?:on|ons)?)?)?\s*(?P<ccy5>{_CCY_PAT})",
]

def _parse_multiplier(mul: Optional[str]) -> float:
    if not mul:
        return 1.0
    return {"M": 1e6, "K": 1e3, "B": 1e9}.get(mul[0].upper(), 1.0)

def _resolve_currency(raw: str) -> Optional[str]:
    return _CURRENCY_ALIASES.get(raw.upper().strip())

def extract_budget_currency(text: str) -> tuple[Optional[float], Optional[str]]:
    results = []
    for pat in _BUDGET_PATTERNS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            gd = m.groupdict()
            amt_str = (
                gd.get("amt1") or gd.get("amt2") or gd.get("amt3")
                or gd.get("amt4") or gd.get("amt5") or ""
            )
            mul_str = (
                gd.get("mul1") or gd.get("mul2") or gd.get("mul3")
                or gd.get("mul4") or gd.get("mul5")
            )
            ccy_raw = (
                gd.get("ccy1") or gd.get("ccy2") or gd.get("ccy3")
                or gd.get("ccy4") or gd.get("ccy5") or ""
            )
            amt_str = re.sub(r"[\s,]", "", amt_str)
            try:
                amt = float(amt_str) * _parse_multiplier(mul_str)
            except ValueError:
                continue
            ccy = _resolve_currency(ccy_raw)
            if ccy and BUDGET_MIN <= amt <= BUDGET_MAX:
                results.append((amt, ccy, m.start()))

    if not results:
        return None, None
    results.sort(key=lambda x: x[2])
    return results[0][0], results[0][1]


# ─────────────────────────────────────────────────────────────────────────────
#  DEADLINE  — anchor-window engine
# ─────────────────────────────────────────────────────────────────────────────

# ── Keyword anchors ───────────────────────────────────────────────────────────
# All phrases that signal "a deadline date is near".
# Written as one big alternation for re.finditer over the flat text.
_KW_PAT = re.compile(
    r"""
    # ── English ─────────────────────────────────────────────────────────────
      \bdeadline\b
    | \bclosing[\s\-]date\b
    | \bsubmission[\s\-](?:date|deadline)\b
    | \bdue[\s\-]date\b
    | \bmust[\s\-]be[\s\-](?:submitted|received|delivered)\b
    | \bnot[\s\-]later[\s\-]than\b
    | \bno[\s\-]later[\s\-]than\b
    | \bon[\s\-]or[\s\-]before\b
    | \blatest[\s\-](?:by|on)\b
    | \breceipt[\s\-]of\b
    | \bbid[\s\-]closing\b
    | \btender[\s\-]closing\b
    | \bproposals?[\s\-]must[\s\-]reach\b
    | \bproposals?[\s\-]are[\s\-]due\b
    | \bexpressions?[\s\-]of[\s\-]interest[\s\-](?:due|deadline|by|must)\b
    | \beoi[\s\-](?:deadline|due|by)\b
    | \bapplication[\s\-]deadline\b
    | \bclosing[\s\-]time\b
    | \bclose[\s\-](?:of[\s\-])?business\b
    | \bsubmit[\s\-](?:by|before|no[\s\-]later)\b
    | \bTime\s*[;:]\s*\d                    # "Time; 4:00 PM" (South Sudan style)
    # ── French ──────────────────────────────────────────────────────────────
    | \bdate[\s\-]limite\b
    | \bdate[\s\-]de[\s\-]cl[oô]ture\b
    | \bremise[\s\-]des[\s\-]offres\b
    | \bd[eé]p[oô]t[\s\-]des[\s\-]offres\b
    | \bau[\s\-]plus[\s\-]tard\b
    | \bavant[\s\-]le\b
    | \bles[\s\-]offres[\s\-]doivent[\s\-][eê]tre[\s\-]re[cç]ues\b
    | \bles[\s\-]candidatures[\s\-]doivent[\s\-][eê]tre[\s\-]soumises\b
    | \bdate[\s\-]limite[\s\-]de[\s\-](?:soumission|d[eé]p[oô]t|remise)\b
    | \bcl[oô]ture[\s\-]des[\s\-](?:offres|ao|appels?)\b
    | \bheure[\s\-]limite\b
    | \bexpressions?[\s\-]d['']\s*int[eé]r[eê]t[\s\-](?:doivent|sont[\s\-]attendues?)\b
    # ── Arabic ──────────────────────────────────────────────────────────────
    | \bالموعد[\s\-]النهائي\b
    | \bتاريخ[\s\-](?:الإغلاق|الاستلام|التقديم)\b
    | \bفي[\s\-]موعد[\s\-]أقصاه\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ── Month names ───────────────────────────────────────────────────────────────
_MONTHS_EN = (
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?"
    r"|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
)
_MONTHS_FR = (
    r"janvier|f[eé]vrier|mars|avril|mai|juin"
    r"|juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre"
)
_MONTHS_AR = (
    r"يناير|فبراير|مارس|أبريل|مايو|يونيو"
    r"|يوليو|أغسطس|سبتمبر|أكتوبر|نوفمبر|ديسمبر"
)
_MONTHS_PAT = rf"(?:{_MONTHS_EN}|{_MONTHS_FR}|{_MONTHS_AR})"

_ORD = r"(?:st|nd|rd|th|ème|eme|er|ère|ere)"  # EN + FR ordinal suffixes

# Day-of-week in English AND French
_WKDAY = (
    r"(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?"
    r"|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?"
    r"|[Ll]undi|[Mm]ardi|[Mm]ercredi|[Jj]eudi|[Vv]endredi|[Ss]amedi|[Dd]imanche)"
)

# ── Full-date pattern (finds date strings in flat text) ───────────────────────
_DATE_PAT = re.compile(
    # ISO:  2026-06-15
    r"\b\d{4}[\/\-\.]\d{1,2}[\/\-\.]\d{1,2}\b"
    # numeric dmy/mdy:  15/06/2026  or  06-15-2026
    r"|\b\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4}\b"
    # AfDB dash style:  28-Apr-2026  /  28 Apr 2026
    rf"|\b\d{{1,2}}[\s\-]{_MONTHS_PAT}[\s\-]\d{{4}}\b"
    # "15 June 2026" / "15th June 2026" / "15 juin 2026" / "15ème juin 2026"
    rf"|\b\d{{1,2}}{_ORD}?\s+{_MONTHS_PAT}\s+\d{{4}}\b"
    # "June 15, 2026" / "juin 15 2026"
    rf"|\b{_MONTHS_PAT}\s+\d{{1,2}}{_ORD}?,?\s+\d{{4}}\b"
    # Day-of-week prefix EN+FR:  "Tuesday, 15 April 2026" / "vendredi 6 mars 2026"
    # The optional ", le" handles "mardi, le 10 mars 2026"
    rf"|\b{_WKDAY},?\s+(?:le\s+)?\d{{1,2}}{_ORD}?\s+{_MONTHS_PAT}\s+\d{{4}}\b",
    re.IGNORECASE,
)

# ── Month-year fallback pattern ("March 2026", "mars 2026") ──────────────────
# Used only if no full date was found near a keyword.
_MONTH_YEAR_PAT = re.compile(
    rf"\b({_MONTHS_PAT})\s+(\d{{4}})\b",
    re.IGNORECASE,
)

# ── Time pattern ─────────────────────────────────────────────────────────────
# Handles:  14:00 / 14h00 / 2:00 PM / 12 noon / midnight
#           "10 heures 00" / "10 heures 00 minute" (French spelled-out)
#           "Time; 4:00 PM" prefix already caught by keyword
_TIME_PAT = re.compile(
    r"\b(\d{1,2})[h:](\d{2})\s*(?:hrs?|GMT|UTC|local\s+time|heure\s+locale|heure\s+de\s+\w+)?\b"
    r"|\b(\d{1,2})\s+heures?\s+(\d{2})\s*(?:minutes?)?\b"   # "10 heures 00 minute"
    r"|\b(12\s*noon|midnight|minuit)\b"
    r"|\b(\d{1,2}:\d{2})\s*(AM|PM)\b",
    re.IGNORECASE,
)

# ── Month translation tables ──────────────────────────────────────────────────
_FR_TO_EN = {
    "janvier": "January",   "février":  "February",  "fevrier":  "February",
    "mars":    "March",     "avril":    "April",      "mai":      "May",
    "juin":    "June",      "juillet":  "July",       "août":     "August",
    "aout":    "August",    "septembre":"September",  "octobre":  "October",
    "novembre":"November",  "décembre": "December",   "decembre": "December",
}
_AR_TO_EN = {
    "يناير":"January",  "فبراير":"February", "مارس":"March",
    "أبريل":"April",    "مايو":"May",         "يونيو":"June",
    "يوليو":"July",     "أغسطس":"August",     "سبتمبر":"September",
    "أكتوبر":"October", "نوفمبر":"November",  "ديسمبر":"December",
}

def _translate_months(s: str) -> str:
    for fr, en in _FR_TO_EN.items():
        s = re.sub(rf"\b{fr}\b", en, s, flags=re.IGNORECASE)
    for ar, en in _AR_TO_EN.items():
        s = s.replace(ar, en)
    return s

def _strip_ordinal(s: str) -> str:
    """Remove EN+FR ordinal suffixes: '1st'→'1', '3ème'→'3'."""
    return re.sub(r"(\d+)(?:st|nd|rd|th|ème|eme|er|ère|ere)\b", r"\1", s, flags=re.IGNORECASE)

def _strip_day_of_week(s: str) -> str:
    """Remove leading day-of-week (EN + FR) including optional ', le'."""
    return re.sub(
        r"^(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?"
        r"|Sat(?:urday)?|Sun(?:day)?"
        r"|[Ll]undi|[Mm]ardi|[Mm]ercredi|[Jj]eudi|[Vv]endredi|[Ss]amedi|[Dd]imanche)"
        r",?\s+(?:le\s+)?",
        "", s, flags=re.IGNORECASE,
    ).strip()

def _extract_time_from_span(text: str, pos: int, window: int = 120) -> Optional[tuple[int, int]]:
    """
    Look for a time value in text[pos-window : pos+window].
    Returns (hour, minute) or None.
    """
    snippet = text[max(0, pos - window) : pos + window]
    m = _TIME_PAT.search(snippet)
    if not m:
        return None
    g = m.groups()
    # Group layout: (h_colon, m_colon, h_spelled, m_spelled, noon_mid, h_ampm, ampm)
    if g[0] and g[1]:                        # HH:MM or HHhMM
        return (int(g[0]), int(g[1]))
    if g[2] and g[3]:                        # "10 heures 00"
        return (int(g[2]), int(g[3]))
    if g[4]:                                 # noon / midnight
        raw = g[4].lower()
        return (12, 0) if "noon" in raw or "midi" in raw else (0, 0)
    if g[5] and g[6]:                        # "2:00 PM"
        h, mi = int(g[5].split(":")[0]), int(g[5].split(":")[1])
        if g[6].upper() == "PM" and h != 12:
            h += 12
        elif g[6].upper() == "AM" and h == 12:
            h = 0
        return (h, mi)
    return None

def _parse_full_date(raw: str) -> Optional[datetime]:
    """
    Parse a full date string to UTC datetime.
    Handles FR/AR month names, ordinals, day-of-week prefixes.
    """
    s = _strip_day_of_week(raw)
    s = _strip_ordinal(s)
    # Remove "le" article before day number: "le 10 mars" → "10 mars"
    s = re.sub(r"^\s*le\s+", "", s, flags=re.IGNORECASE).strip()
    s = _translate_months(s)
    try:
        dt = dateutil_parser.parse(s, dayfirst=True)
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, OverflowError):
        return None


def extract_deadline(text: str) -> Optional[datetime]:
    """
    Anchor-window deadline extraction.

    Algorithm:
      1. Collect all keyword anchor positions (character offsets).
      2. Collect all full-date candidate positions + the matched string.
      3. For each date candidate, find the nearest keyword anchor.
         Score = HIGH if distance ≤ DEADLINE_WINDOW, LOW otherwise.
      4. Parse every date candidate into a UTC datetime.
      5. Apply time component from the window around the date position.
      6. Validate: must be a FUTURE date within 2 years.
      7. Return the EARLIEST valid future date (among all candidates,
         regardless of score). Scores only break ties at the same date.
    """
    # 1. Keyword anchor positions
    kw_positions: list[int] = [m.start() for m in _KW_PAT.finditer(text)]

    # 2. Full-date candidates: (char_pos, matched_string)
    date_candidates: list[tuple[int, str]] = [
        (m.start(), m.group(0)) for m in _DATE_PAT.finditer(text)
    ]

    def _nearest_kw_distance(pos: int) -> int:
        """Minimum character distance from pos to any keyword anchor."""
        if not kw_positions:
            return 10_000
        return min(abs(pos - kp) for kp in kw_positions)

    # 3 + 4 + 5 + 6 — build list of (datetime, score)
    valid: list[tuple[datetime, int]] = []

    for pos, raw in date_candidates:
        dist  = _nearest_kw_distance(pos)
        score = 2 if dist <= DEADLINE_WINDOW else 1

        dt = _parse_full_date(raw)
        if dt is None:
            continue

        # Apply time from a ±120-char window around the date
        time_tuple = _extract_time_from_span(text, pos)
        if time_tuple:
            h, mi = time_tuple
            dt = dt.replace(hour=h, minute=mi, second=0)
        else:
            dt = dt.replace(hour=23, minute=59, second=59)

        valid.append((dt, score))

    if valid:
        # ── DEBUG: print all candidates so you can verify the regex ─────────
        for _dt, _sc in sorted(valid, key=lambda x: x[0]):
            import sys
            print(f"  [deadline_candidate] score={_sc} dt={_dt.strftime('%Y-%m-%d %H:%M')}",
                  file=sys.stderr)
        # ── END DEBUG (remove before production) ────────────────────────────

        # Among all candidates, return the EARLIEST date that is in the future.
        # Falls back to the globally earliest date if nothing is in the future
        # (this allows past-date testing while still preferring future dates).
        future = [(dt, sc) for dt, sc in valid if dt > NOW]
        pool   = future if future else valid
        pool.sort(key=lambda x: (x[0], -x[1]))
        return pool[0][0]

    # ── Month-year fallback ───────────────────────────────────────────────────
    # "Bidding documents available from March 2026" → last day of that month.
    # Only used when NO full-date was found near a keyword.
    for m in _MONTH_YEAR_PAT.finditer(text):
        dist  = _nearest_kw_distance(m.start())
        score = 2 if dist <= DEADLINE_WINDOW else 1

        month_str = _translate_months(m.group(1))
        year_str  = m.group(2)
        try:
            # Parse as 1st of month, then advance to last day
            dt = dateutil_parser.parse(f"1 {month_str} {year_str}", dayfirst=True)
            last_day = calendar.monthrange(dt.year, dt.month)[1]
            dt = dt.replace(day=last_day, hour=23, minute=59, second=59,
                            tzinfo=timezone.utc)
        except (ValueError, OverflowError):
            continue

        return dt

    return None


def compute_days_to_deadline(deadline: Optional[datetime]) -> Optional[int]:
    """
    Calendar days from now to deadline. Always recomputed — never cached.
    Negative = deadline already passed (e.g. -3 means expired 3 days ago).
    """
    if deadline is None:
        return None
    return (deadline - NOW).days


# ─────────────────────────────────────────────────────────────────────────────
#  LANGUAGE DETECTION
# ─────────────────────────────────────────────────────────────────────────────

_LANG_KEYWORDS: dict[str, list[str]] = {
    "English": [
        "the", "and", "for", "request", "proposal", "invitation", "bid",
        "procurement", "tender", "contract", "services", "goods", "works",
        "please", "submit", "deadline", "closing", "project", "expressions",
        "interest", "applicants", "eligible", "shall", "hereby",
    ],
    "French": [
        "le", "la", "les", "de", "du", "pour", "offre", "appel", "date",
        "limite", "marché", "fourniture", "travaux", "soumission", "dossier",
        "entreprise", "prestataire", "montant", "projet", "contrat",
        "consultants", "candidature", "financement", "bénéficiaire",
    ],
    "Arabic": [
        "مناقصة", "عطاء", "طلب", "عروض", "مشروع", "خدمات",
        "توريد", "أعمال", "تقديم", "الموعد", "النهائي",
        "العقد", "المشتريات", "المقاول",
    ],
    "Portuguese": [
        "para", "dos", "das", "projeto", "proposta", "concurso",
        "fornecimento", "obras", "serviços", "prazo", "contrato",
        "licitação", "adjudicação", "entidade", "financiamento",
    ],
    "Spanish": [
        "para", "los", "las", "proyecto", "propuesta", "licitación",
        "suministro", "obras", "servicios", "plazo", "contrato",
        "adquisición", "entidad", "financiamiento", "oferta",
    ],
}

def extract_language(text: str) -> str:
    """
    Detect document language by keyword frequency.
    Always returns a string — defaults to English if inconclusive.
    """
    lower = text.lower()
    scores: dict[str, int] = {}
    for lang, keywords in _LANG_KEYWORDS.items():
        if lang == "Arabic":
            scores[lang] = sum(1 for kw in keywords if kw in text)
        else:
            scores[lang] = sum(
                1 for kw in keywords
                if re.search(rf"\b{re.escape(kw)}\b", lower)
            )

    best_lang  = max(scores, key=scores.get)
    best_score = scores[best_lang]

    if best_score < 3:
        return DEFAULT_LANGUAGE

    # Require a gap of ≥ 3 to distinguish Portuguese from Spanish
    if best_lang in ("Portuguese", "Spanish"):
        other = "Spanish" if best_lang == "Portuguese" else "Portuguese"
        if abs(scores[best_lang] - scores[other]) < 3:
            return DEFAULT_LANGUAGE

    return best_lang


# ─────────────────────────────────────────────────────────────────────────────
#  CONTACT — email + phone only  (name removed)
# ─────────────────────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
)

_PHONE_RAW_RE = re.compile(
    r"(?<!\d)(\+?\d[\d\s\.\-\(\)\/]{6,20}\d)(?!\d)"
)

def _normalize_phone(raw: str) -> Optional[str]:
    """Normalize to E.164: strip non-digits, keep leading +, validate length."""
    has_plus = raw.strip().startswith("+")
    digits   = re.sub(r"\D", "", raw)
    if not (7 <= len(digits) <= 15):
        return None
    return ("+" if has_plus else "") + digits

def extract_contacts(text: str) -> dict:
    """Extract email and phone (E.164). Name extraction removed."""
    email_m = _EMAIL_RE.search(text)
    email   = email_m.group(0) if email_m else None

    phone = None
    for m in _PHONE_RAW_RE.finditer(text):
        normalized = _normalize_phone(m.group(1))
        if normalized:
            phone = normalized
            break

    return {"email": email, "phone": phone}


# ─────────────────────────────────────────────────────────────────────────────
#  PROCUREMENT GROUP
# ─────────────────────────────────────────────────────────────────────────────

_PROC_GROUP_RULES: list[tuple[str, list[str]]] = [
    ("CONSULTING", [
        r"\bconsult(?:ing|ancy|ant)s?\b", r"\btechnical\s+assistance\b", r"\bTA\b",
        r"\bexperts?\b", r"\baudits?\b", r"\bsupervi(?:sion|sor)\b",
        r"\bdue\s+diligence\b", r"\bfeasibility\s+stud(?:y|ies)\b",
        r"\bmanagement\s+(?:support|services?)\b", r"\bcapacity\s+building\b",
        r"\btraining\s+(?:services?|program(?:me)?)\b",
        r"\bmonitoring\s+(?:and\s+)?evaluation\b", r"\bM&E\b",
        # French
        r"\bconsultants?\b", r"\bconsultance\b",
        r"\bprestations?\s+intellectuelles?\b", r"\b[eé]tudes?\b",
        r"\bassistance\s+technique\b", r"\bformation\b",
        r"\baudit\b", r"\bsupervision\b",
        # Arabic
        r"\bاستشارات\b", r"\bخبراء\b", r"\bمساعدة\s+تقنية\b",
    ]),
    ("WORKS", [
        r"\bconstruct(?:ion|ing)\b", r"\brehabilitat(?:ion|ing)\b",
        r"\binfrastructure\b", r"\bcivil\s+works?\b", r"\brenovation\b",
        r"\binstallation\b", r"\bbuilding\b", r"\bbridge\b",
        r"\broad(?:s|way)?\b", r"\bdam\b", r"\birrigation\b",
        r"\bsanitation\b", r"\belectrification\b",
        # French
        r"\btravaux\b", r"\bréhabilitation\b", r"\bconstruction\b",
        r"\bgénie\s+civil\b", r"\bouvrage(?:s)?\b",
        r"\baménagement\b", r"\bassainissement\b", r"\bélectrification\b",
        # Arabic
        r"\bأعمال\s+مدنية\b", r"\bإنشاء\b", r"\bتشييد\b",
    ]),
    ("NON-CONSULTING", [
        r"\bnon[\s\-]consult\w+\b", r"\boperational\s+services?\b",
        r"\bcleaning\s+services?\b", r"\bsecurity\s+services?\b",
        r"\btransport(?:ation)?\s+services?\b", r"\bmaintenance\s+services?\b",
        r"\bcatering\b", r"\bguard(?:ing)?\s+services?\b",
        r"\bwaste\s+management\b",
        # French
        r"\bservices?\s+non[\s\-]intellectuels?\b", r"\bentretien\b",
        r"\bnettoyage\b", r"\bsécurité\s+(?:privée|des\s+locaux)\b",
        r"\bgestion\s+des\s+déchets\b",
    ]),
    ("GOODS", [
        r"\bgoods?\b", r"\bequipment\b", r"\bsuppl(?:y|ies|ier)\b",
        r"\bvehicles?\b", r"\bcomputers?\b",
        r"\bmedical\s+(?:equipment|supplies|devices?)\b",
        r"\bprocurement\s+of\b", r"\bpurchase\s+of\b",
        r"\bdelivery\s+of\b", r"\bsupply\s+and\s+delivery\b",
        r"\bIT\s+equipment\b",
        # French
        r"\bfournitures?\b", r"\bmatériels?\b",
        r"\bacquisition\s+(?:de\s+)?(?:biens?|matériels?|équipements?)\b",
        r"\blivraison\b",
        # Arabic
        r"\bتوريد\b", r"\bبضائع\b", r"\bمعدات\b",
    ]),
]

def classify_proc_group(text: str) -> Optional[str]:
    lower = text.lower()
    for group, patterns in _PROC_GROUP_RULES:
        for pat in patterns:
            if re.search(pat, lower, re.IGNORECASE):
                return group
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

def normalize_structured_fields(raw: dict) -> dict:
    out = {}

    # Budget
    budget = raw.get("budget")
    if budget is not None:
        try:
            budget = float(str(budget).replace(",", "").replace(" ", ""))
            out["budget"] = budget if BUDGET_MIN <= budget <= BUDGET_MAX else None
        except (ValueError, TypeError):
            out["budget"] = None
    else:
        out["budget"] = None

    # Currency
    ccy = raw.get("currency")
    out["currency"] = str(ccy).upper()[:10] if ccy else None

    # Deadline
    dl = raw.get("deadline_datetime")
    if isinstance(dl, str):
        try:
            dl = dateutil_parser.parse(dl)
        except Exception:
            dl = None
    if dl is not None:
        if dl.tzinfo is None:
            dl = dl.replace(tzinfo=timezone.utc)
        # Accept any parsed date — past or future — as long as it's a real date.
        # We store expired deadlines too so the dashboard can show them.
        # Only hard-reject dates absurdly far in the future (> 2 years).
        out["deadline_datetime"] = dl if dl <= DEADLINE_MAX_FUTURE else None
    else:
        out["deadline_datetime"] = None

    # Days to deadline — always recomputed
    out["days_to_deadline"] = compute_days_to_deadline(out["deadline_datetime"])

    # Language — always a string
    lang = raw.get("language")
    out["language"] = lang if lang in ALLOWED_LANGUAGES else DEFAULT_LANGUAGE

    # Contact
    out["contact_email"] = (raw.get("contact_email") or "")[:300] or None
    out["contact_phone"] = (raw.get("contact_phone") or "")[:50]  or None

    # Procurement group
    pg = raw.get("procurement_group")
    out["procurement_group"] = pg if pg in ALLOWED_PROC_GROUPS else None

    return out


# ─────────────────────────────────────────────────────────────────────────────
#  CORE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_structured_fields(tender_dict: dict) -> StructuredExtraction:
    title       = tender_dict.get("title_clean") or ""
    notice_text = tender_dict.get("notice_text")  or ""
    full_text   = _combine_text(title, notice_text)

    result = StructuredExtraction()

    result.budget, result.currency = extract_budget_currency(full_text)
    if result.budget is None:
        result._warnings.append("budget/currency not found")

    result.deadline_datetime = extract_deadline(full_text)
    result.days_to_deadline  = compute_days_to_deadline(result.deadline_datetime)
    if result.deadline_datetime is None:
        result._warnings.append("deadline not found or invalid")

    result.language = extract_language(full_text)

    contacts = extract_contacts(full_text)
    result.contact_email = contacts.get("email")
    result.contact_phone = contacts.get("phone")
    if not result.contact_email:
        result._warnings.append("contact email not found")

    result.procurement_group = (
        classify_proc_group(title) or classify_proc_group(full_text)
    )
    if result.procurement_group is None:
        result._warnings.append("procurement group could not be classified")

    return result


# ─────────────────────────────────────────────────────────────────────────────
#  MERGE RULE
# ─────────────────────────────────────────────────────────────────────────────

def merge_with_existing(existing: dict, extracted: dict) -> dict:
    """
    Normalized (existing) wins over rule-extracted values.
    days_to_deadline always comes from extracted (recomputed daily).
    """
    merged = {}
    for key in extracted:
        if key == "days_to_deadline":
            merged[key] = extracted[key]
            continue
        ev = existing.get(key)
        merged[key] = ev if (ev is not None and ev != "") else extracted.get(key)
    return merged


# ─────────────────────────────────────────────────────────────────────────────
#  DB UPDATE
# ─────────────────────────────────────────────────────────────────────────────

def update_enriched_tender(session, enriched_id: int, merged_dict: dict) -> None:
    try:
        from models import EnrichedTender
    except ImportError:
        log.error("Could not import EnrichedTender model — skipping DB write")
        return

    enriched = session.get(EnrichedTender, enriched_id)
    if enriched is None:
        log.warning("  No EnrichedTender row found for id=%s — skipping", enriched_id)
        return

    for k, v in merged_dict.items():
        if hasattr(enriched, k):
            setattr(enriched, k, v)

    if enriched.deadline_datetime:
        enriched.days_to_deadline = (enriched.deadline_datetime - NOW).days

    enriched.enrichment_status = "rules_complete"
    enriched.enriched_at       = NOW

    session.merge(enriched)
    session.commit()
    log.info("  ✓ Updated EnrichedTender id=%s", enriched_id)


# ─────────────────────────────────────────────────────────────────────────────
#  PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def process_tender(tender_dict: dict, dry_run: bool = False) -> dict:
    extraction = extract_structured_fields(tender_dict)

    raw_dict = {
        "budget":            extraction.budget,
        "currency":          extraction.currency,
        "deadline_datetime": extraction.deadline_datetime,
        "days_to_deadline":  extraction.days_to_deadline,
        "language":          extraction.language,
        "contact_email":     extraction.contact_email,
        "contact_phone":     extraction.contact_phone,
        "procurement_group": extraction.procurement_group,
    }

    normalized_extracted = normalize_structured_fields(raw_dict)

    existing = {
        k.replace("existing_", ""): v
        for k, v in tender_dict.items() if k.startswith("existing_")
    }

    merged = merge_with_existing(existing, normalized_extracted)

    # Always recompute days_to_deadline from whichever deadline won
    winning_dl = merged.get("deadline_datetime") or existing.get("deadline_datetime")
    merged["days_to_deadline"] = compute_days_to_deadline(winning_dl)

    for w in extraction._warnings:
        log.debug("  [warn] %s", w)

    if not dry_run:
        try:
            from db import get_session
            with get_session() as session:
                update_enriched_tender(
                    session, tender_dict["enriched_tender_id"], merged
                )
        except Exception as e:
            log.error(
                "  DB write failed for enriched_tender_id=%s: %s",
                tender_dict.get("enriched_tender_id"), e,
            )

    return merged


# ─────────────────────────────────────────────────────────────────────────────
#  BATCH RUN
# ─────────────────────────────────────────────────────────────────────────────

def run_rule_extraction(
    dry_run: bool = False,
    limit: int | None = None,
    portals: list[str] | None = None,
) -> None:
    try:
        from db import get_session
        from models import EnrichedTender, Tender
        from sqlalchemy import select
    except ImportError as e:
        log.error("Import error — run from project root: %s", e)
        return

    portals  = [p.lower() for p in (portals or DEFAULT_PORTALS)]
    counters = dict(total=0, success=0, failed=0)

    with get_session() as session:
        stmt = (
            select(EnrichedTender, Tender)
            .join(Tender, EnrichedTender.tender_id == Tender.id)
            .where(
                EnrichedTender.enrichment_status == "seeded",
                EnrichedTender.source_portal.in_(portals),
            )
            .order_by(EnrichedTender.tender_id)
        )
        if limit:
            stmt = stmt.limit(limit)

        rows = session.execute(stmt).all()
        tender_dicts = [
            {
                "enriched_tender_id":         et.id,
                "tender_id":                  et.tender_id,
                "title_clean":                et.title_clean or t.title,
                "notice_text":                t.notice_text,
                "existing_budget":            et.budget,
                "existing_currency":          et.currency,
                "existing_deadline_datetime": et.deadline_datetime,
                "existing_days_to_deadline":  et.days_to_deadline,
                "existing_language":          et.language,
                "existing_contact_email":     et.contact_email,
                "existing_contact_phone":     et.contact_phone,
                "existing_procurement_group": et.procurement_group,
            }
            for et, t in rows
        ]

    counters["total"] = len(tender_dicts)
    log.info(
        "Found %d seeded rows pending rule extraction (portals: %s)",
        counters["total"], ", ".join(portals),
    )

    if counters["total"] == 0:
        log.info("Nothing to do — run stage2a first to seed EnrichedTender rows.")
        return

    for td in tender_dicts:
        log.info(
            "Processing enriched_tender_id=%s (tender_id=%s)",
            td["enriched_tender_id"], td["tender_id"],
        )
        try:
            merged = process_tender(td, dry_run=dry_run)
            if dry_run:
                log.info("  [DRY-RUN] %s", merged)
            counters["success"] += 1
        except Exception as e:
            log.error(
                "  Failed enriched_tender_id=%s: %s",
                td.get("enriched_tender_id"), e, exc_info=True,
            )
            counters["failed"] += 1

    log.info(
        "Rule extraction done — total=%d  success=%d  failed=%d%s",
        counters["total"], counters["success"], counters["failed"],
        "  [DRY-RUN]" if dry_run else "",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  SELF-TEST — real AfDB notice patterns
# ─────────────────────────────────────────────────────────────────────────────

_TEST_TENDERS = [
    # ── 1. RCA (French) — "au plus tard le vendredi 6 mars 2026 à 17h00"
    #       Two deadlines: 6 March (clarifications) and 10 March (EOI).
    #       Earliest = 6 March must be returned.
    {
        "enriched_tender_id": 1, "tender_id": 1001,
        "title_clean": "Recrutement d'un consultant individuel — RCA",
        "notice_text": (
            "Les demandes d'éclaircissements doivent être adressées à la Coordination "
            "par courrier électronique au plus tard le vendredi 6 mars 2026 à 17h00.\n"
            "Les expressions d'intérêt doivent être déposées au plus tard mardi, "
            "le 10 mars 2026 à 17h00 (heure de Bangui).\n"
            "Phone: +236 72 42 25 30\n"
            "Email: beangaiprosper@yahoo.fr\n"
            "Financement: FAPA"
        ),
        "existing_budget": None, "existing_currency": None,
        "existing_deadline_datetime": None, "existing_days_to_deadline": None,
        "existing_language": None, "existing_contact_email": None,
        "existing_contact_phone": None, "existing_procurement_group": None,
    },
    # ── 2. Liberia (English) — UA + USD amounts, no submission deadline (GPN)
    #       Month-year fallback: "March 2026" → 31 March 2026.
    {
        "enriched_tender_id": 2, "tender_id": 1002,
        "title_clean": "General Procurement Notice — ISEDRMP Liberia",
        "notice_text": (
            "Loan: UA 13.83 Million equivalent to USD 18.30 Million.\n"
            "Bidding documents are expected to be available in March 2026.\n"
            "Emails: mlombeh@mfdp.gov.lr / ctoe@mfdp.gov.lr\n"
            "Phone: +231770212332"
        ),
        "existing_budget": None, "existing_currency": None,
        "existing_deadline_datetime": None, "existing_days_to_deadline": None,
        "existing_language": None, "existing_contact_email": None,
        "existing_contact_phone": None, "existing_procurement_group": None,
    },
    # ── 3. South Sudan (English) — ordinal "March 20th 2026", "Time; 4:00 P.M."
    {
        "enriched_tender_id": 3, "tender_id": 1003,
        "title_clean": "Individual Consultant — Debt Management Expert",
        "notice_text": (
            "Expressions of interest must be delivered or e-mailed to the address "
            "below by March 20th 2026, Time; 4:00 P.M. Central African Time.\n"
            "Tel: +211923497444\n"
            "Email: emmanuelmoyaagya@gmail.com"
        ),
        "existing_budget": None, "existing_currency": None,
        "existing_deadline_datetime": None, "existing_days_to_deadline": None,
        "existing_language": None, "existing_contact_email": None,
        "existing_contact_phone": None, "existing_procurement_group": None,
    },
    # ── 4. Côte d'Ivoire (French) — "lundi 17 mars 2026 à 10 heures 00 minute"
    {
        "enriched_tender_id": 4, "tender_id": 1004,
        "title_clean": "Recrutement firme audit financier — Côte d'Ivoire",
        "notice_text": (
            "Les expressions d'intérêt doivent être déposées sous plis fermés "
            "ou transmises par courriel au plus tard le lundi 17 mars 2026 "
            "à 10 heures 00 minute (heure locale).\n"
            "Tel: +225 07 59 50 11 19\n"
            "E-mail: projetdmp2023@gmail.com"
        ),
        "existing_budget": None, "existing_currency": None,
        "existing_deadline_datetime": None, "existing_days_to_deadline": None,
        "existing_language": None, "existing_contact_email": None,
        "existing_contact_phone": None, "existing_procurement_group": None,
    },
    # ── 5. Cabo Verde PDE table — label and date on separate lines
    #       "Tender/proposals closing date … September 25, 2025"
    #       This date is in the past → should return None (already expired).
    {
        "enriched_tender_id": 5, "tender_id": 1005,
        "title_clean": "Individual Consultant — Cape Verde Technology Park",
        "notice_text": (
            "Tender/proposals closing date/Date de Clôture\n"
            "AO/Consultation:\n"
            "September 25, 2025\n"
            "Contract amount: EUR 12,618.00\n"
            "Email: maguy.santos12@gmail.com\n"
            "Tel: +238 994 52 27"
        ),
        "existing_budget": None, "existing_currency": None,
        "existing_deadline_datetime": None, "existing_days_to_deadline": None,
        "existing_language": None, "existing_contact_email": None,
        "existing_contact_phone": None, "existing_procurement_group": None,
    },
    # ── 6. IGAD/GCF GPN — "available from June 2026" → month-year fallback
    {
        "enriched_tender_id": 6, "tender_id": 1006,
        "title_clean": "GCF BREFOL — General Procurement Notice",
        "notice_text": (
            "Bidding documents are expected to be available from June 2026.\n"
            "Phone: +25321333723\n"
            "Email: Feto.Esimo@igad.int"
        ),
        "existing_budget": None, "existing_currency": None,
        "existing_deadline_datetime": None, "existing_days_to_deadline": None,
        "existing_language": None, "existing_contact_email": None,
        "existing_contact_phone": None, "existing_procurement_group": None,
    },
    # ── 7. World Bank / GOODS — GBP 3.2M, "12 noon GMT", normalized wins
    {
        "enriched_tender_id": 7, "tender_id": 1007,
        "title_clean": "Supply and Delivery of Medical Equipment",
        "notice_text": (
            "Estimated Contract Value: GBP 3.2M\n"
            "Submission Deadline: Bids must be received by 10 August 2026, 12 noon GMT.\n"
            "Email: procurement@nha.gov.gh\n"
            "Tel: +233 30 276 1000"
        ),
        "existing_budget": None, "existing_currency": None,
        "existing_deadline_datetime": None, "existing_days_to_deadline": None,
        "existing_language": "English",  # normalized wins
        "existing_contact_email": None, "existing_contact_phone": None,
        "existing_procurement_group": None,
    },
]


def run_self_test() -> None:
    print("\n" + "=" * 82)
    print("  SELF-TEST — stage2b v3 (anchor-window deadline engine)")
    print("=" * 82)

    # Expected outcomes per test (None = expect None is acceptable)
    expectations = {
        1: {"deadline_month": 3,  "deadline_day": 6,  "deadline_year": 2026,
            "currency": None,     "procurement_group": "CONSULTING"},
        2: {"deadline_month": 3,  "deadline_day": 31, "deadline_year": 2026,
            "currency": "UA",     "procurement_group": "CONSULTING"},
        3: {"deadline_month": 3,  "deadline_day": 20, "deadline_year": 2026,
            "currency": None,     "procurement_group": "CONSULTING"},
        4: {"deadline_month": 3,  "deadline_day": 17, "deadline_year": 2026,
            "currency": None,     "procurement_group": "CONSULTING"},
        5: {"deadline_month": None,                                    # past date
            "currency": "EUR",    "procurement_group": None},
        6: {"deadline_month": 6,  "deadline_day": 30, "deadline_year": 2026,
            "currency": None,     "procurement_group": None},
        7: {"deadline_month": 8,  "deadline_day": 10, "deadline_year": 2026,
            "currency": "GBP",    "procurement_group": "GOODS"},
    }

    all_pass = True
    for td in _TEST_TENDERS:
        eid = td["enriched_tender_id"]
        print(f"\n{'─'*82}")
        print(f"  ID    : {eid}")
        print(f"  Title : {td['title_clean'][:78]}")
        print(f"{'─'*82}")

        merged = process_tender(td, dry_run=True)
        exp    = expectations[eid]
        passed = True

        # Deadline check
        dl = merged.get("deadline_datetime")
        if exp.get("deadline_month") is None:
            ok = dl is None
        else:
            ok = (
                dl is not None
                and dl.month == exp["deadline_month"]
                and dl.day   == exp["deadline_day"]
                and dl.year  == exp["deadline_year"]
            )
        mark = "✓" if ok else "⚠"
        passed = passed and ok
        dl_str = dl.strftime("%Y-%m-%d %H:%M UTC") if dl else "None"
        exp_dl = (f"{exp.get('deadline_year')}-{exp.get('deadline_month'):02d}-"
                  f"{exp.get('deadline_day'):02d}"
                  if exp.get("deadline_month") else "None")
        print(f"  {mark}  deadline_datetime       {dl_str:<30} [expected {exp_dl}]")

        # Days to deadline
        dtd = merged.get("days_to_deadline")
        print(f"     days_to_deadline       {str(dtd):<30}")

        # Currency check
        ccy = merged.get("currency")
        ok  = ccy == exp.get("currency")
        mark = "✓" if ok else "⚠"
        passed = passed and ok
        print(f"  {mark}  currency               {str(ccy):<30} [expected {exp.get('currency')}]")

        # Procurement group check
        pg  = merged.get("procurement_group")
        ok  = pg == exp.get("procurement_group")
        mark = "✓" if ok else "⚠"
        passed = passed and ok
        print(f"  {mark}  procurement_group      {str(pg):<30} [expected {exp.get('procurement_group')}]")

        # Language + contacts (informational)
        print(f"     language               {merged.get('language')}")
        print(f"     contact_email          {merged.get('contact_email')}")
        print(f"     contact_phone          {merged.get('contact_phone')}")

        all_pass = all_pass and passed
        print(f"\n  Result: {'✓ PASS' if passed else '⚠ FAIL'}")

    print("\n" + "=" * 82)
    print(f"  Overall: {'✓ ALL TESTS PASSED' if all_pass else '⚠ SOME TESTS FAILED'}")
    print("=" * 82 + "\n")



# ─────────────────────────────────────────────────────────────────────────────
#  DIAGNOSE MODE — reads DB, prints ALL date candidates (past + future)
#  No DB writes. No date filtering. Use to verify regex coverage.
#  Remove or disable once satisfied with deadline extraction quality.
# ─────────────────────────────────────────────────────────────────────────────

_STATUS_ICON = {"future": "✓", "past": "⏎", "parse_failed": "✗"}

def _deadline_candidates(text: str) -> list[dict]:
    """
    Run the anchor-window engine with ZERO date filtering.
    Returns every candidate (past or future) with full diagnostic info.
    """
    kw_positions = [m.start() for m in _KW_PAT.finditer(text)]

    def _dist(pos: int) -> int:
        return min((abs(pos - kp) for kp in kw_positions), default=99_999)

    results = []

    # Full-date candidates
    for m in _DATE_PAT.finditer(text):
        pos  = m.start()
        raw  = m.group(0)
        dist = _dist(pos)
        dt   = _parse_full_date(raw)
        t    = _extract_time_from_span(text, pos) if dt else None
        if dt and t:
            dt = dt.replace(hour=t[0], minute=t[1], second=0)
        elif dt:
            dt = dt.replace(hour=23, minute=59, second=59)
        snip_s = max(0, pos - 40)
        results.append({
            "type":       "full_date",
            "raw":        raw,
            "parsed_dt":  dt,
            "pos":        pos,
            "dist":       dist,
            "anchored":   dist <= DEADLINE_WINDOW,
            "has_time":   t is not None,
            "is_future":  dt is not None and dt > NOW,
            "status":     ("parse_failed" if dt is None
                           else "future" if dt > NOW else "past"),
            "snippet":    text[snip_s: pos + 40].replace("\n", " ").strip(),
        })

    # Month-year fallback candidates
    for m in _MONTH_YEAR_PAT.finditer(text):
        pos  = m.start()
        dist = _dist(pos)
        ms   = _translate_months(m.group(1))
        dt   = None
        try:
            tmp  = dateutil_parser.parse(f"1 {ms} {m.group(2)}", dayfirst=True)
            last = calendar.monthrange(tmp.year, tmp.month)[1]
            dt   = tmp.replace(day=last, hour=23, minute=59,
                               second=59, tzinfo=timezone.utc)
        except Exception:
            pass
        snip_s = max(0, pos - 40)
        results.append({
            "type":       "month_year",
            "raw":        m.group(0),
            "parsed_dt":  dt,
            "pos":        pos,
            "dist":       dist,
            "anchored":   dist <= DEADLINE_WINDOW,
            "has_time":   False,
            "is_future":  dt is not None and dt > NOW,
            "status":     ("parse_failed" if dt is None
                           else "future" if dt > NOW else "past"),
            "snippet":    text[snip_s: pos + 40].replace("\n", " ").strip(),
        })

    results.sort(key=lambda x: x["pos"])
    return results


def _pick_best(candidates: list[dict]) -> dict | None:
    """Same selection logic as production extract_deadline."""
    anchored = [c for c in candidates if c["anchored"] and c["parsed_dt"]]
    future_a = [c for c in anchored   if c["is_future"]]
    pool     = future_a if future_a else anchored if anchored else [
        c for c in candidates if c["parsed_dt"]
    ]
    return min(pool, key=lambda x: x["parsed_dt"]) if pool else None


def run_diagnose(
    limit:   int | None  = None,
    portals: list[str]   = None,
    export:  str | None  = None,
    missing_only: bool   = False,
) -> None:
    """
    Read tenders from DB and print ALL deadline candidates for each,
    including past dates. No writes. Use to measure regex coverage.
    """
    try:
        from db import get_session
        from models import Tender
        from sqlalchemy import select
    except ImportError as e:
        log.error("Import error — run from project root: %s", e)
        return

    import csv as _csv

    portals = [p.lower() for p in (portals or DEFAULT_PORTALS)]

    with get_session() as session:
        stmt = (
            select(Tender)
            .where(Tender.source_portal.in_(portals))
            .order_by(Tender.id)
        )
        if limit:
            stmt = stmt.limit(limit)
        tenders = session.execute(stmt).scalars().all()

    total        = len(tenders)
    found_future = 0
    found_past   = 0
    found_none   = 0
    csv_rows     = []

    print(f"\nScanning {total} tenders from portals: {', '.join(portals)}")
    print("ALL dates shown — past and future — no filtering\n")

    for t in tenders:
        text = " ".join(filter(None, [t.title or "", t.notice_text or ""]))
        if not text.strip():
            found_none += 1
            continue

        candidates = _deadline_candidates(text)
        best       = _pick_best(candidates)

        if missing_only and candidates:
            continue

        # Counters
        if not candidates:
            found_none += 1
        elif best and best["is_future"]:
            found_future += 1
        else:
            found_past += 1

        # Print
        print(f"\n{'═'*80}")
        print(f"  tender_id : {t.id}  |  portal: {t.source_portal}")
        print(f"  title     : {(t.title or '')[:72]}")
        print(f"  {'─'*78}")

        if not candidates:
            print("  ⚠  NO CANDIDATES — keyword or date pattern not matching")
        else:
            best_dt = best['parsed_dt'].strftime('%Y-%m-%d %H:%M') if best else '—'
            print(f"  {len(candidates)} candidate(s)   →  selected: {best_dt}")
            print()
            for c in candidates:
                icon   = _STATUS_ICON.get(c["status"], "?")
                anchor = "anchored" if c["anchored"] else f"dist={c['dist']}"
                timed  = "+time" if c["has_time"] else ""
                sel    = " ← SELECTED" if best is not None and c is best else ""
                dt_s   = c["parsed_dt"].strftime("%Y-%m-%d %H:%M") if c["parsed_dt"] else "PARSE FAIL"
                print(f"  {icon} [{c['type']:<10}] [{anchor:<14}] {dt_s}  {timed}{sel}")
                print(f"       raw : '{c['raw']}'")
                print(f"       ctx : '…{c['snippet']}…'")

        # CSV rows
        if export:
            if not candidates:
                csv_rows.append({
                    "tender_id": t.id, "portal": t.source_portal,
                    "title": (t.title or '')[:100],
                    "type": "", "raw": "", "parsed_dt": "",
                    "status": "no_candidate", "anchored": "",
                    "dist": "", "has_time": "", "selected": "",
                    "snippet": "",
                })
            for c in candidates:
                csv_rows.append({
                    "tender_id": t.id,
                    "portal":    t.source_portal,
                    "title":     (t.title or '')[:100],
                    "type":      c["type"],
                    "raw":       c["raw"],
                    "parsed_dt": c["parsed_dt"].strftime("%Y-%m-%d %H:%M") if c["parsed_dt"] else "",
                    "status":    c["status"],
                    "anchored":  c["anchored"],
                    "dist":      c["dist"],
                    "has_time":  c["has_time"],
                    "selected":  best is not None and c is best,
                    "snippet":   c["snippet"],
                })

    # Summary
    pct = round((found_future + found_past) / total * 100, 1) if total else 0
    print(f"\n{'═'*80}")
    print(f"  SUMMARY")
    print(f"  {'─'*78}")
    print(f"  Total scanned            : {total}")
    print(f"  ✓ Deadline found (future): {found_future}")
    print(f"  ⏎ Deadline found (past)  : {found_past}")
    print(f"  ⚠ No deadline found      : {found_none}")
    print(f"  Detection rate           : {pct}%  ({found_future + found_past}/{total})")
    print(f"  {'═'*78}\n")

    if export and csv_rows:
        with open(export, "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=csv_rows[0].keys())
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"  CSV exported → {export}\n")

# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rule-based extraction on notice_text to fill gaps in EnrichedTender."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract and print results without writing to DB.")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Process at most N tenders.")
    parser.add_argument("--portals", nargs="+", default=DEFAULT_PORTALS, metavar="PORTAL",
                        help="Source portals (default: afdb worldbank undp ungm).")
    parser.add_argument("--test", action="store_true",
                        help="Run self-test against 7 real AfDB notice patterns (no DB).")
    parser.add_argument("--diagnose", action="store_true",
                        help="Print ALL deadline candidates (past+future) from DB. No writes.")
    parser.add_argument("--missing-only", action="store_true",
                        help="With --diagnose: only show tenders where NO deadline was found.")
    parser.add_argument("--export", default=None, metavar="FILE.csv",
                        help="With --diagnose: export all candidates to CSV.")
    args = parser.parse_args()

    if args.test:
        run_self_test()
    elif args.diagnose:
        run_diagnose(
            limit        = args.limit,
            portals      = args.portals,
            export       = args.export,
            missing_only = args.missing_only,
        )
    else:
        run_rule_extraction(dry_run=args.dry_run, limit=args.limit, portals=args.portals)