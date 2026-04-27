"""
enricher/procurement_group.py
==============================
Standalone procurement group extraction from tender titles.


Determines procurement group for all portals.
For AfDB and WorldBank the normalization pipeline usually
fills this field already — this only fills gaps where it is null.


Groups:
    CONSULTING      — intellectual services: studies, assessments,
                      consultancies, evaluations, technical assistance
    WORKS           — physical construction, renovation, installation
    GOODS           — supply and delivery of physical items
    NON-CONSULTING  — operational services: catering, cleaning,
                      security, lease, events
    Others          — could not be determined from title


Rules:
    - NEVER returns None or null — always returns a string
    - If existing value is already populated, returns existing (no overwrite)
    - If nothing matches, returns "Others"
    - Priority order: GOODS (supply+install) → WORKS → CONSULTING → NON-CONSULTING → GOODS → Others


Usage (import):
    from procurement_group import extract_procurement_group
    group = extract_procurement_group(title)


    from procurement_group import get_procurement_group
    group = get_procurement_group(title, existing, portal)


Run directly to update the database:
    python enricher/procurement_group.py                      # fill NULL and 'Others' (default)
    python enricher/procurement_group.py --nulls-only         # only NULL rows
    python enricher/procurement_group.py --others-only        # only 'Others' rows
    python enricher/procurement_group.py --all                # re-extract every row
    python enricher/procurement_group.py --dry-run            # print only, no DB writes
    python enricher/procurement_group.py --limit 100          # first 100 rows only
"""


import argparse
import logging
import os
import re
import sys
from typing import Optional


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


BATCH_SIZE = 200




# ─────────────────────────────────────────────────────────────────────────────
#  GOODS+WORKS HYBRID PATTERNS (checked first)
#  Titles like "Fourniture et installation" are GOODS, not WORKS.
#  These patterns catch supply+installation combos before WORKS patterns fire.
# ─────────────────────────────────────────────────────────────────────────────


_GOODS_INSTALL_PATTERNS: list[str] = [
    r"\bsupply\s+and\s+(?:delivery\s+and\s+)?installation\b",
    r"\bsupply,?\s+delivery\s+and\s+installation\b",
    r"\bfourniture\s+et\s+installation\b",
    r"\bfourniture\s+et\s+pose\b",
    r"\bacquisition\s+et\s+installation\b",
    r"\bprocurement\s+and\s+installation\b",
]




# ─────────────────────────────────────────────────────────────────────────────
#  WORKS PATTERNS
# ─────────────────────────────────────────────────────────────────────────────


_WORKS_PATTERNS: list[str] = [
    # English
    r"\bconstruct(?:ion|ing)\b",
    r"\brehabilitat(?:ion|ing)\b",
    r"\brenovation\b",
    r"\brepair\s+works?\b",
    r"\bcivil\s+works?\b",
    r"\bbuilding\s+works?\b",
    r"\bsite\s+preparation\b",
    r"\binstallation\s+of\b",
    r"\binstall(?:ation|ing)\b",
    r"\bdemolition\b",
    r"\bexcavation\b",
    r"\bdrilling\b",
    r"\bboreholes?\b",
    r"\bpaving\b",
    r"\bfencing\b",
    r"\belectrical\s+works?\b",
    r"\bplumbing\b",
    r"\bsolar\s+(?:system\s+)?(?:design\s+and\s+)?installation\b",
    r"\brainwater\s+harvesting\b",
    r"\binfrastructure\s+(?:works?|development|project)\b",
    r"\broad\s+(?:works?|construction|rehabilitation)\b",
    r"\bbridge\s+(?:construction|rehabilitation)\b",
    r"\birrigation\s+(?:works?|scheme|infrastructure)\b",
    r"\bwater\s+(?:supply\s+)?(?:works?|network|system\s+construction)\b",
    r"\bsanitation\s+works?\b",
    r"\blatrine\s+construction\b",
    r"\brefurbishment\b",
    r"\boutfitting\b",
    r"\blandscaping\s+works?\b",
    r"\bforage\b",
    # French
    r"\btravaux\b",
    r"\bréhabilitation\b",
    r"\bconstruction\s+de\b",
    r"\baménagement\b",
    r"\bgénie\s+civil\b",
    r"\bréalisation\s+des\s+travaux\b",
    r"\bréparation\b",
    r"\bréfection\b",
    r"\bvoirie\b",
    r"\bassainissement\b",
    r"\bforages?\b",
    r"\badduction\s+d['e]\s*eau\b",
    # Spanish
    r"\bconstrucci[oó]n\b",
    r"\brehabilitaci[oó]n\b",
    r"\breparaci[oó]n\b",
    r"\bobras?\s+civiles?\b",
    r"\binfrastructura\b",
    r"\bperforaci[oó]n\b",
    # Portuguese
    r"\bconstrução\b",
    r"\breabilitação\b",
    r"\bobras\b",
    r"\bperfuração\b",
    r"\bsaneamento\b",
]




# ─────────────────────────────────────────────────────────────────────────────
#  NON-CONSULTING OVERRIDE PATTERNS (checked BEFORE consulting)
#  Catches titles like "Consultancy Firm for Provision of Security Services"
#  where a consulting keyword appears but the actual service is non-consulting.
#  These are specific enough to safely short-circuit CONSULTING detection.
# ─────────────────────────────────────────────────────────────────────────────

_NON_CONSULTING_OVERRIDE_PATTERNS: list[str] = [
    # Security / guarding
    r"\bprovision\s+of\s+security\s+services?\b",
    r"\bsecurity\s+(?:guard|guarding|services?)\b",
    r"\bguarding\s+services?\b",
    r"\bgardiennage\b",                               # French
    r"\bseguridad\s+(?:privada|servicios?)\b",        # Spanish
    # Cleaning / janitorial
    r"\bprovision\s+of\s+cleaning\s+services?\b",
    r"\bcleaning\s+services?\b",
    r"\bjanitorial\s+services?\b",
    r"\bnettoyage\b",                                  # French
    r"\blimpieza\s+(?:de|servicios?)?\b",              # Spanish
    # Catering / food / hospitality
    r"\bprovision\s+of\s+catering\b",
    r"\bcatering\s+services?\b",
    r"\bhotel\s+(?:accommodation|services?|facilities|booking)\b",
    r"\baccommodation\s+(?:and\s+)?(?:services?|facilities)\b",
    r"\bh[eé]bergement\b",                            # French
    r"\brestauración\b",                               # Spanish
    r"\brestauration\b",                               # French
    # Events / conferences / travel
    r"\bconference\s+(?:package|venue|facilities|services?|organisation|organization)\b",
    r"\bevent\s+(?:management|organisation|organization|services?)\b",
    r"\borganis(?:ation|ing)\s+(?:of\s+)?(?:\w+\s+){0,3}(?:event|conference|seminar|meeting)\b",
    r"\borganiz(?:ation|ing)\s+(?:of\s+)?(?:\w+\s+){0,3}(?:event|conference|seminar|meeting)\b",
    r"\btravel\s+(?:management|agency|services?|arrangements?)\b",
    r"\bvenue\s+(?:hire|rental|booking)\b",
    r"\bstaff\s+retreat\b",
    r"\bretreat\b",
    # Transport / logistics / freight
    r"\bfreight\s+forwarding\b",
    r"\blogistics\s+services?\b",
    r"\btransport(?:ation)?\s+services?\b",
    r"\btransporte\s+(?:de|servicios?)?\b",           # Spanish/Portuguese
    r"\bvehicle\s+(?:hire|rental|lease)\b",
    r"\bcar\s+(?:hire|rental)\b",
    r"\bfret\s+(?:a[eé]rien|maritime)\b",              # French freight
    r"\blogística\b",                                  # Spanish
    # Insurance
    r"\binsurance\s+(?:plans?|services?|coverage|policy|policies)\b",
    r"\bassurance\s+(?:maladie|vie|groupe|tous\s+risques)\b",  # French
    r"\bseguros?\s+(?:de|médico|colectivo)?\b",        # Spanish
    # Office / facilities / lease
    r"\boffice\s+(?:space|rent|lease)\b",
    r"\bpremises\s+(?:hire|rental|lease)\b",
    r"\blocation\s+de\s+(?:bureau|véhicule|salle|locaux)\b",   # French lease
    r"\balquiler\s+de\b",                              # Spanish rental
    # Printing / reproduction
    r"\bprinting\s+(?:and\s+)?(?:services?|production|of)\b",
    r"\bimpression\s+(?:de|et)\b",                    # French
    r"\bimpresi[oó]n\s+de\b",                         # Spanish
    # Maintenance / repair (operational, not construction)
    r"\bmaintenance\s+services?\b",
    r"\bpreventive\s+maintenance\b",
    r"\bcorrective\s+maintenance\b",
    r"\brepair\s+(?:and\s+)?maintenance\b",
    r"\bentretien\s+(?:et\s+)?(?:maintenance|réparation)\b",   # French
    r"\bmantenimiento\s+(?:de|preventivo|correctivo)?\b",      # Spanish
    # Waste / environment operations
    r"\bwaste\s+(?:collection|management|disposal)\b",
    r"\bgardening\s+services?\b",
    r"\blandscaping\s+services?\b",
    # Translation / interpretation (operational language service)
    r"\btranslation\s+services?\b",
    r"\binterpreting\s+services?\b",
    r"\binterpretation\s+services?\b",
    r"\btraduction\s+(?:de|et)\b",                    # French
    r"\btraducci[oó]n\s+(?:de|e)\b",                  # Spanish
    # Courier / postal
    r"\bcourier\s+services?\b",
    r"\bpostal\s+services?\b",
    r"\bmessagerie\b",                                 # French courier
]


# ─────────────────────────────────────────────────────────────────────────────
#  CONSULTING PATTERNS
# ─────────────────────────────────────────────────────────────────────────────


_CONSULTING_PATTERNS: list[str] = [
    # English
    r"\bconsult(?:ing|ant|ancy|ance)s?\b",
    r"\bindividual\s+consultant\b",
    r"\btechnical\s+assistance\b",
    r"\bexperts?\b",
    r"\bfeasibility\s+study\b",
    r"\bassessment\b",
    r"\bevaluation\b",
    r"\breview\b",
    r"\bstudy\b",
    r"\bsurvey\b",
    r"\bmapping\b",
    r"\bvaluation\b",
    r"\bactuarial\b",
    r"\baudit\b",
    r"\bfacilitation\b",
    r"\bcoaching\b",
    r"\bmentoring\b",
    r"\badvisory\b",
    r"\bresearch\b",
    r"\btraining\s+(?:services?|programme|program|course|workshop)\b",
    r"\bworkshop\b",
    r"\bcapacity\s+(?:building|development)\b",
    r"\bmonitoring\s+and\s+evaluation\b",
    r"\bM&E\b",
    r"\bbaseline\b",
    r"\bendline\b",
    r"\bimpact\s+(?:evaluation|assessment)\b",
    r"\bperformance\s+(?:review|evaluation)\b",
    r"\bpolicy\s+(?:development|reform|analysis|framework|advisory)\b",
    r"\bstrategic\s+plan\b",
    r"\binstitutional\s+support\b",
    r"\bprogramme?\s+(?:management|support|coordination)\b",
    r"\bpurchase\s+of\s+services\s+related\s+to\s+(?:training|technical)\b",
    r"\bengagement\s+for\b",
    r"\bdeliver\s+\w+\s+programme\b",
    r"\bdeliver\s+tech\b",
    r"\borganization\s+to\s+deliver\b",
    r"\bcompany\s+to\s+deliver\b",
    # Governance / Public Sector
    r"\bgovernance\b",
    r"\bpublic\s+(?:financial\s+)?management\b",
    r"\bfiscal\s+(?:management|reform|policy)\b",
    r"\bpfm\b",
    r"\bpublic\s+sector\s+reform\b",
    r"\binstitutional\s+reform\b",
    r"\banti[-\s]?corruption\b",
    r"\btransparency\b",
    r"\baccountability\b",
    # Strategy / Policy / Advisory
    r"\bstrateg(?:y|ic|ies)\b",
    r"\broadmap\b",
    r"\baction\s+plan\b",
    r"\bmaster\s+plan\b",
    r"\bregulatory\s+(?:framework|reform|review)\b",
    r"\bwhite\s+paper\b",
    # Financial / Economic consulting
    r"\bfinancial\s+(?:management|reform|advisory|analysis)\b",
    r"\bgreen\s+finance\b",
    r"\bdebt\s+management\b",
    r"\bbudget\s+(?:reform|support)\b",
    r"\btax\s+(?:policy|reform|administration)\b",
    # Institutional / Capacity / Advisory support
    r"\binstitutional\s+(?:support|strengthening|development)\b",
    r"\borganizational\s+(?:development|review|assessment)\b",
    r"\bstrengthening\b",
    # Plans / Frameworks / Diagnostic (covers "Design a Biodiversity Finance Plan" etc.)
    r"\b(?:adaptation|action|finance|development|recovery|resilience|investment|implementation)\s+plan\b",
    r"\bnational\s+\w+\s+plan\b",
    r"\b(?:develop(?:ment\s+of)?|design(?:ing)?|draft(?:ing)?|elaborat(?:ion|ing)|formulat(?:ion|ing)|compil(?:ing|ation\s+of)|prepar(?:ation\s+of|ing))\s+(?:a\s+|an\s+|the\s+)?\w[\w\s]{0,40}(?:plan|strategy|framework|policy|roadmap|guidelines?|protocol|methodology|report|assessment|review|study|analysis|manual|curriculum)\b",
    r"\bframework\b",
    r"\bbenchmarking\b",
    r"\bdiagnostic\b",
    r"\banalysis\b",
    r"\bsupport\s+to\b",
    r"\bdevelopment\s+of\b",
    r"\bdesign\s+(?:of|and)\b",
    r"\bdrafting\s+(?:of|a|an|the)\b",
    r"\belaboration\b",
    r"\bformulation\b",
    r"\bcompilation\b",
    # Digital / Transformation / Software / Platform (covers DCMS, web portals, dashboards)
    r"\bdigital\s+(?:transformation|strategy|governance|economy|credential|registry|platform)\b",
    r"\be[-\s]?government\b",
    r"\binnovation\s+(?:strategy|advisory)\b",
    r"\bwebsite\s+(?:development|design|redesign)\b",
    r"\bweb\s+(?:portal|platform|application|app)\s+(?:development|design)\b",
    r"\bplatform\s+development\b",
    r"\bsystem\s+development\b",
    r"\bsoftware\s+(?:development|solution|system)\b",
    r"\bdashboard\s+(?:development|design)\b",
    r"\bdatabase\s+(?:development|design|management\s+system)\b",
    r"\bintegrated\s+\w[\w\s]{0,30}(?:system|platform|solution)\b",
    r"\bdeployment\s+(?:and\s+\w+\s+)?(?:of\s+)?(?:a\s+)?(?:system|platform|solution)\b",
    r"\boperationali[sz]ation\b",
    r"\bdigital\s+solution\b",
    r"\bICT\s+(?:solution|system|platform|development)\b",
    # French
    r"\b[eé]tude\b",
    r"\bassistance\s+technique\b",
    r"\bformation\s+(?:professionnelle|technique|en)\b",
    r"\bréalisation\s+d(?:'|e\s+une)\s+[eé]tude\b",
    r"\brecrutement\s+d(?:'|e\s+un)\b",
    r"\bappel\s+[aà]\s+collaborateur\b",
    r"\bprestation\s+intellectuelle\b",
    r"\bconsultoría\b",
    # Spanish
    r"\bconsutor(?:ía|ia)\b",
    r"\basistencia\s+técnica\b",
]




# ─────────────────────────────────────────────────────────────────────────────
#  NON-CONSULTING PATTERNS
# ─────────────────────────────────────────────────────────────────────────────


_NON_CONSULTING_PATTERNS: list[str] = [
    # English
    r"\bcatering\b",
    r"\bevent\s+services?\b",
    r"\blease\s+of\b",
    r"\boffice\s+space\b",
    r"\bcleaning\s+services?\b",
    r"\bsecurity\s+(?:guard|services?)\b",
    r"\bmaintenance\s+services?\b",
    r"\bguarding\s+services?\b",
    r"\bprinting\s+services?\b",
    r"\bjanitorial\b",
    r"\bwaste\s+collection\b",
    r"\btransport(?:ation)?\s+services?\b",
    r"\bcourier\s+services?\b",
    r"\binsurance\s+(?:plans?|services?|coverage)\b",
    r"\bstaff\s+retreat\b",
    r"\bretreat\b",
    r"\bconference\s+(?:services?|facilities)\b",
    r"\bfacilitation\s+of\b",
    r"\btravel\s+(?:management|agency|services?)\b",
    r"\btranslation\s+services?\b",
    r"\binterpreting\s+services?\b",
    r"\bvenue\s+(?:hire|rental)\b",
    r"\bcar\s+(?:hire|rental)\b",
    r"\bfreight\s+forwarding\b",
    r"\blogistics\s+services?\b",
    # French
    r"\blocation\s+de\b",
    r"\bnettoyage\b",
    r"\bgardiennage\b",
    r"\brestauration\b",
    r"\bh[eé]bergement\b",
    r"\bmessagerie\b",
    r"\btraduction\b",
    r"\bimpression\s+de\b",
    r"\bentretien\b",
    r"\bservices?\s+de\s+maintenance\b",         # explicit phrase match
    r"\bservices?\s+d['']entretien\b",  
    # Spanish
    r"\btransporte\s+de\b",
    r"\blogística\b",
    r"\balquiler\s+de\b",
    r"\bmantenimiento\b",
    r"\btraducci[oó]n\b",
    r"\bimpresi[oó]n\s+de\b",
    r"\brestauraci[oó]n\s+(?:de\s+)?(?:servicio|alimento)\b",
    # Portuguese
    r"\btranslação\b",
    r"\bserviços\s+de\s+(?:limpeza|segurança|transporte|catering|tradução)\b",
]




# ─────────────────────────────────────────────────────────────────────────────
#  GOODS PATTERNS
# ─────────────────────────────────────────────────────────────────────────────


_GOODS_PATTERNS: list[str] = [
    # English
    r"\bsupply\s+(?:and\s+)?(?:delivery|of)\b",
    r"\bdelivery\s+of\b",
    r"\bprocurement\s+of\b",
    r"\bpurchase\s+of\b",
    r"\bsupply\s+of\b",
    r"\bprovision\s+of\s+(?:equipment|goods|materials)\b",
    r"\bsupplies\b",
    r"\bequipment\b",
    r"\bvehicles?\b",
    r"\bfurniture\b",
    r"\bcomputers?\b",
    r"\blaptops?\b",
    r"\bnotebooks?\b",
    r"\bmedical\s+(?:supplies|equipment|devices?)\b",
    r"\bpharmaceuticals?\b",
    r"\bmedicines?\b",
    r"\bmaterials?\b",
    r"\btools?\b",
    r"\bkits?\b",
    r"\bPPE\b",
    r"\buniforms?\b",
    r"\bspare\s+parts?\b",
    r"\bICT\s+equipment\b",
    r"\boffice\s+supplies\b",
    r"\bseeds?\b",
    r"\bfertili[zs]ers?\b",
    r"\bpesticides?\b",
    r"\bagrochemicals?\b",
    r"\bfuel\b",
    r"\bgenerators?\b",
    # French
    r"\bachat\b",
    r"\bfournitures?\b",
    r"\bmatériels?\b",
    r"\bacquisition\s+(?:de|d')\b",
    r"\blivraison\b",
    r"\bfornitura\b",                           # Italian/Portuguese tender term
    # Spanish
    r"\bsuministro\b",
    r"\badquisici[oó]n\b",
    r"\bcompra\s+de\b",
    r"\binsumos\b",
    r"\bequipos?\b",
    r"\bmateriales\b",
    r"\bsemillas\b",
    r"\bfertilizantes\b",
    r"\bbienes\b",
    # Portuguese
    r"\baquisição\b",
    r"\bequipamentos?\b",
    r"\bmateriais\b",
    r"\bfornecimento\s+de\b",
    r"\baquisição\s+de\b",
]




# ─────────────────────────────────────────────────────────────────────────────
#  EXTRACTION FUNCTIONS (importable)
# ─────────────────────────────────────────────────────────────────────────────


def extract_procurement_group(title: str) -> str:
    """
    Extract procurement group from a tender title.
    Always returns a string — never None or null.
    Returns one of: CONSULTING | WORKS | GOODS | NON-CONSULTING | Others
    """
    if not title or not title.strip():
        return "Others"


    lower = title.strip().lower()


    for pat in _GOODS_INSTALL_PATTERNS:
        if re.search(pat, lower, re.IGNORECASE):
            return "GOODS"


    for pat in _WORKS_PATTERNS:
        if re.search(pat, lower, re.IGNORECASE):
            return "WORKS"

    # Check for NON-CONSULTING overrides BEFORE consulting patterns fire.
    # This prevents "Consultancy Firm for Security Services" → CONSULTING.
    for pat in _NON_CONSULTING_OVERRIDE_PATTERNS:
        if re.search(pat, lower, re.IGNORECASE):
            return "NON-CONSULTING"

    for pat in _CONSULTING_PATTERNS:
        if re.search(pat, lower, re.IGNORECASE):
            return "CONSULTING"


    for pat in _NON_CONSULTING_PATTERNS:
        if re.search(pat, lower, re.IGNORECASE):
            return "NON-CONSULTING"


    for pat in _GOODS_PATTERNS:
        if re.search(pat, lower, re.IGNORECASE):
            return "GOODS"


    return "Others"




def get_procurement_group(
    title:    str,
    existing: Optional[str] = None,
    portal:   Optional[str] = None,
) -> str:
    """
    Get procurement group with merge rules.
    Always returns a string — never None or null.
    If existing is already populated, keeps it (no overwrite).
    """
    if existing and existing.strip():
        return existing
    return extract_procurement_group(title)




# ─────────────────────────────────────────────────────────────────────────────
#  DATABASE UPDATE RUNNER
# ─────────────────────────────────────────────────────────────────────────────


def run_update(
    mode:    str = "nulls-and-others",
    dry_run: bool = False,
    limit:   int | None = None,
) -> None:
    """
    Update procurement_group on enriched_tenders rows.
    Runs independently — does not touch enrichment_status.
    """
    try:
        from db import SessionLocal
        from sqlalchemy import text
    except ImportError as e:
        log.error("Import error (db): %s", e)
        return


    if mode == "all":
        where = ""
    elif mode == "nulls-only":
        where = "WHERE e.procurement_group IS NULL"
    elif mode == "others-only":
        where = "WHERE e.procurement_group = 'Others'"
    else:
        where = "WHERE e.procurement_group IS NULL OR e.procurement_group = 'Others'"


    limit_clause = f"LIMIT {limit}" if limit else ""


    select_sql = f"""
        SELECT e.id, t.title, e.procurement_group
        FROM enriched_tenders e
        JOIN tenders t ON e.tender_id = t.id
        {where}
        ORDER BY e.id
        {limit_clause}
    """


    total    = 0
    counters = dict(updated=0, failed=0)


    session = SessionLocal()
    try:
        rows  = session.execute(text(select_sql)).mappings().all()
        total = len(rows)
        log.info("Found %d rows to process (mode: %s)", total, mode)


        if total == 0:
            log.info("Nothing to do.")
            return


        batch_params = []


        for row in rows:
            new_group = extract_procurement_group(row["title"] or "")


            if dry_run:
                log.info(
                    "  [DRY-RUN] id=%-6s  old=%-20s  new=%-20s  title=%s",
                    row["id"],
                    row["procurement_group"] or "NULL",
                    new_group,
                    (row["title"] or "")[:80],
                )
                counters["updated"] += 1
                continue


            batch_params.append({"procurement_group": new_group, "id": row["id"]})


            if len(batch_params) >= BATCH_SIZE:
                try:
                    session.execute(
                        text("UPDATE enriched_tenders SET procurement_group = :procurement_group WHERE id = :id"),
                        batch_params,
                    )
                    session.commit()
                    counters["updated"] += len(batch_params)
                    log.info("  ... committed %d rows", len(batch_params))
                except Exception as e:
                    session.rollback()
                    log.error("  ✗ Batch failed: %s — %s", type(e).__name__, str(e))
                    counters["failed"] += len(batch_params)
                batch_params = []


        if batch_params:
            try:
                session.execute(
                    text("UPDATE enriched_tenders SET procurement_group = :procurement_group WHERE id = :id"),
                    batch_params,
                )
                session.commit()
                counters["updated"] += len(batch_params)
                log.info("  ... committed final %d rows", len(batch_params))
            except Exception as e:
                session.rollback()
                log.error("  ✗ Final batch failed: %s — %s", type(e).__name__, str(e))
                counters["failed"] += len(batch_params)


    except Exception as e:
        session.rollback()
        log.error("Fatal error: %s — %s", type(e).__name__, str(e), exc_info=True)
    finally:
        session.close()


    log.info(
        "Done — total=%d  updated=%d  failed=%d%s",
        total, counters["updated"], counters["failed"],
        "  [DRY-RUN]" if dry_run else "",
    )




# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract and update procurement_group on enriched_tenders."
    )


    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--all", dest="mode", action="store_const", const="all",
        help="Re-extract and update every row.")
    mode_group.add_argument("--nulls-only", dest="mode", action="store_const", const="nulls-only",
        help="Only fill rows where procurement_group IS NULL.")
    mode_group.add_argument("--others-only", dest="mode", action="store_const", const="others-only",
        help="Only update rows where procurement_group = 'Others'.")
    mode_group.add_argument("--nulls-and-others", dest="mode", action="store_const", const="nulls-and-others",
        help="Fill NULL and 'Others' rows (default).")
    parser.set_defaults(mode="nulls-and-others")


    parser.add_argument("--dry-run", action="store_true",
        help="Print what would be updated without writing to DB.")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
        help="Process at most N rows.")


    args = parser.parse_args()
    run_update(mode=args.mode, dry_run=args.dry_run, limit=args.limit)