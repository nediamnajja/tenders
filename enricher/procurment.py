# """
# enricher/procurement_group.py
# ==============================
# Standalone procurement group extraction from tender titles.


# Determines procurement group for all portals.
# For AfDB and WorldBank the normalization pipeline usually
# fills this field already — this only fills gaps where it is null.


# Groups:
#     CONSULTING      — intellectual services: studies, assessments,
#                       consultancies, evaluations, technical assistance
#     WORKS           — physical construction, renovation, installation
#     GOODS           — supply and delivery of physical items
#     NON-CONSULTING  — operational services: catering, cleaning,
#                       security, lease, events
#     Others          — could not be determined from title


# Rules:
#     - NEVER returns None or null — always returns a string
#     - If existing value is already populated, returns existing (no overwrite)
#     - If nothing matches, returns "Others"
#     - Priority order: GOODS (supply+install) → WORKS → CONSULTING → NON-CONSULTING → GOODS → Others


# Usage (import):
#     from procurement_group import extract_procurement_group
#     group = extract_procurement_group(title)


#     from procurement_group import get_procurement_group
#     group = get_procurement_group(title, existing, portal)


# Run directly to update the database:
#     python enricher/procurement_group.py                      # fill NULL and 'Others' (default)
#     python enricher/procurement_group.py --nulls-only         # only NULL rows
#     python enricher/procurement_group.py --others-only        # only 'Others' rows
#     python enricher/procurement_group.py --all                # re-extract every row
#     python enricher/procurement_group.py --dry-run            # print only, no DB writes
#     python enricher/procurement_group.py --limit 100          # first 100 rows only
# """


# import argparse
# import logging
# import os
# import re
# import sys
# from typing import Optional


# ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# if ROOT_DIR not in sys.path:
#     sys.path.insert(0, ROOT_DIR)


# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
# )
# log = logging.getLogger(__name__)


# BATCH_SIZE = 200




# # ─────────────────────────────────────────────────────────────────────────────
# #  GOODS+WORKS HYBRID PATTERNS (checked first)
# #  Titles like "Fourniture et installation" are GOODS, not WORKS.
# #  These patterns catch supply+installation combos before WORKS patterns fire.
# # ─────────────────────────────────────────────────────────────────────────────


# _GOODS_INSTALL_PATTERNS: list[str] = [
#     r"\bsupply\s+and\s+(?:delivery\s+and\s+)?installation\b",
#     r"\bsupply,?\s+delivery\s+and\s+installation\b",
#     r"\bfourniture\s+et\s+installation\b",
#     r"\bfourniture\s+et\s+pose\b",
#     r"\bacquisition\s+et\s+installation\b",
#     r"\bprocurement\s+and\s+installation\b",
# ]




# # ─────────────────────────────────────────────────────────────────────────────
# #  WORKS PATTERNS
# # ─────────────────────────────────────────────────────────────────────────────


# _WORKS_PATTERNS: list[str] = [
#     # English
#     r"\bconstruct(?:ion|ing)\b",
#     r"\brehabilitat(?:ion|ing)\b",
#     r"\brenovation\b",
#     r"\brepair\s+works?\b",
#     r"\bcivil\s+works?\b",
#     r"\bbuilding\s+works?\b",
#     r"\bsite\s+preparation\b",
#     r"\binstallation\s+of\b",
#     r"\binstall(?:ation|ing)\b",
#     r"\bdemolition\b",
#     r"\bexcavation\b",
#     r"\bdrilling\b",
#     r"\bboreholes?\b",
#     r"\bpaving\b",
#     r"\bfencing\b",
#     r"\belectrical\s+works?\b",
#     r"\bplumbing\b",
#     r"\bsolar\s+(?:system\s+)?(?:design\s+and\s+)?installation\b",
#     r"\brainwater\s+harvesting\b",
#     r"\binfrastructure\s+(?:works?|development|project)\b",
#     r"\broad\s+(?:works?|construction|rehabilitation)\b",
#     r"\bbridge\s+(?:construction|rehabilitation)\b",
#     r"\birrigation\s+(?:works?|scheme|infrastructure)\b",
#     r"\bwater\s+(?:supply\s+)?(?:works?|network|system\s+construction)\b",
#     r"\bsanitation\s+works?\b",
#     r"\blatrine\s+construction\b",
#     r"\brefurbishment\b",
#     r"\boutfitting\b",
#     r"\blandscaping\s+works?\b",
#     r"\bforage\b",
#     # French
#     r"\btravaux\b",
#     r"\bréhabilitation\b",
#     r"\bconstruction\s+de\b",
#     r"\baménagement\b",
#     r"\bgénie\s+civil\b",
#     r"\bréalisation\s+des\s+travaux\b",
#     r"\bréparation\b",
#     r"\bréfection\b",
#     r"\bvoirie\b",
#     r"\bassainissement\b",
#     r"\bforages?\b",
#     r"\badduction\s+d['e]\s*eau\b",
#     # Spanish
#     r"\bconstrucci[oó]n\b",
#     r"\brehabilitaci[oó]n\b",
#     r"\breparaci[oó]n\b",
#     r"\bobras?\s+civiles?\b",
#     r"\binfrastructura\b",
#     r"\bperforaci[oó]n\b",
#     # Portuguese
#     r"\bconstrução\b",
#     r"\breabilitação\b",
#     r"\bobras\b",
#     r"\bperfuração\b",
#     r"\bsaneamento\b",
# ]




# # ─────────────────────────────────────────────────────────────────────────────
# #  NON-CONSULTING OVERRIDE PATTERNS (checked BEFORE consulting)
# #  Catches titles like "Consultancy Firm for Provision of Security Services"
# #  where a consulting keyword appears but the actual service is non-consulting.
# #  These are specific enough to safely short-circuit CONSULTING detection.
# # ─────────────────────────────────────────────────────────────────────────────

# _NON_CONSULTING_OVERRIDE_PATTERNS: list[str] = [
#     # Security / guarding
#     r"\bprovision\s+of\s+security\s+services?\b",
#     r"\bsecurity\s+(?:guard|guarding|services?)\b",
#     r"\bguarding\s+services?\b",
#     r"\bgardiennage\b",                               # French
#     r"\bseguridad\s+(?:privada|servicios?)\b",        # Spanish
#     # Cleaning / janitorial
#     r"\bprovision\s+of\s+cleaning\s+services?\b",
#     r"\bcleaning\s+services?\b",
#     r"\bjanitorial\s+services?\b",
#     r"\bnettoyage\b",                                  # French
#     r"\blimpieza\s+(?:de|servicios?)?\b",              # Spanish
#     # Catering / food / hospitality
#     r"\bprovision\s+of\s+catering\b",
#     r"\bcatering\s+services?\b",
#     r"\bhotel\s+(?:accommodation|services?|facilities|booking)\b",
#     r"\baccommodation\s+(?:and\s+)?(?:services?|facilities)\b",
#     r"\bh[eé]bergement\b",                            # French
#     r"\brestauración\b",                               # Spanish
#     r"\brestauration\b",                               # French
#     # Events / conferences / travel
#     r"\bconference\s+(?:package|venue|facilities|services?|organisation|organization)\b",
#     r"\bevent\s+(?:management|organisation|organization|services?)\b",
#     r"\borganis(?:ation|ing)\s+(?:of\s+)?(?:\w+\s+){0,3}(?:event|conference|seminar|meeting)\b",
#     r"\borganiz(?:ation|ing)\s+(?:of\s+)?(?:\w+\s+){0,3}(?:event|conference|seminar|meeting)\b",
#     r"\btravel\s+(?:management|agency|services?|arrangements?)\b",
#     r"\bvenue\s+(?:hire|rental|booking)\b",
#     r"\bstaff\s+retreat\b",
#     r"\bretreat\b",
#     # Transport / logistics / freight
#     r"\bfreight\s+forwarding\b",
#     r"\blogistics\s+services?\b",
#     r"\btransport(?:ation)?\s+services?\b",
#     r"\btransporte\s+(?:de|servicios?)?\b",           # Spanish/Portuguese
#     r"\bvehicle\s+(?:hire|rental|lease)\b",
#     r"\bcar\s+(?:hire|rental)\b",
#     r"\bfret\s+(?:a[eé]rien|maritime)\b",              # French freight
#     r"\blogística\b",                                  # Spanish
#     # Insurance
#     r"\binsurance\s+(?:plans?|services?|coverage|policy|policies)\b",
#     r"\bassurance\s+(?:maladie|vie|groupe|tous\s+risques)\b",  # French
#     r"\bseguros?\s+(?:de|médico|colectivo)?\b",        # Spanish
#     # Office / facilities / lease
#     r"\boffice\s+(?:space|rent|lease)\b",
#     r"\bpremises\s+(?:hire|rental|lease)\b",
#     r"\blocation\s+de\s+(?:bureau|véhicule|salle|locaux)\b",   # French lease
#     r"\balquiler\s+de\b",                              # Spanish rental
#     # Printing / reproduction
#     r"\bprinting\s+(?:and\s+)?(?:services?|production|of)\b",
#     r"\bimpression\s+(?:de|et)\b",                    # French
#     r"\bimpresi[oó]n\s+de\b",                         # Spanish
#     # Maintenance / repair (operational, not construction)
#     r"\bmaintenance\s+services?\b",
#     r"\bpreventive\s+maintenance\b",
#     r"\bcorrective\s+maintenance\b",
#     r"\brepair\s+(?:and\s+)?maintenance\b",
#     r"\bentretien\s+(?:et\s+)?(?:maintenance|réparation)\b",   # French
#     r"\bmantenimiento\s+(?:de|preventivo|correctivo)?\b",      # Spanish
#     # Waste / environment operations
#     r"\bwaste\s+(?:collection|management|disposal)\b",
#     r"\bgardening\s+services?\b",
#     r"\blandscaping\s+services?\b",
#     # Translation / interpretation (operational language service)
#     r"\btranslation\s+services?\b",
#     r"\binterpreting\s+services?\b",
#     r"\binterpretation\s+services?\b",
#     r"\btraduction\s+(?:de|et)\b",                    # French
#     r"\btraducci[oó]n\s+(?:de|e)\b",                  # Spanish
#     # Courier / postal
#     r"\bcourier\s+services?\b",
#     r"\bpostal\s+services?\b",
#     r"\bmessagerie\b",                                 # French courier
# ]


# # ─────────────────────────────────────────────────────────────────────────────
# #  CONSULTING PATTERNS
# # ─────────────────────────────────────────────────────────────────────────────


# _CONSULTING_PATTERNS: list[str] = [
#     # English
#     r"\bconsult(?:ing|ant|ancy|ance)s?\b",
#     r"\bindividual\s+consultant\b",
#     r"\btechnical\s+assistance\b",
#     r"\bexperts?\b",
#     r"\bfeasibility\s+study\b",
#     r"\bassessment\b",
#     r"\bevaluation\b",
#     r"\breview\b",
#     r"\bstudy\b",
#     r"\bsurvey\b",
#     r"\bmapping\b",
#     r"\bvaluation\b",
#     r"\bactuarial\b",
#     r"\baudit\b",
#     r"\bfacilitation\b",
#     r"\bcoaching\b",
#     r"\bmentoring\b",
#     r"\badvisory\b",
#     r"\bresearch\b",
#     r"\btraining\s+(?:services?|programme|program|course|workshop)\b",
#     r"\bworkshop\b",
#     r"\bcapacity\s+(?:building|development)\b",
#     r"\bmonitoring\s+and\s+evaluation\b",
#     r"\bM&E\b",
#     r"\bbaseline\b",
#     r"\bendline\b",
#     r"\bimpact\s+(?:evaluation|assessment)\b",
#     r"\bperformance\s+(?:review|evaluation)\b",
#     r"\bpolicy\s+(?:development|reform|analysis|framework|advisory)\b",
#     r"\bstrategic\s+plan\b",
#     r"\binstitutional\s+support\b",
#     r"\bprogramme?\s+(?:management|support|coordination)\b",
#     r"\bpurchase\s+of\s+services\s+related\s+to\s+(?:training|technical)\b",
#     r"\bengagement\s+for\b",
#     r"\bdeliver\s+\w+\s+programme\b",
#     r"\bdeliver\s+tech\b",
#     r"\borganization\s+to\s+deliver\b",
#     r"\bcompany\s+to\s+deliver\b",
#     # Governance / Public Sector
#     r"\bgovernance\b",
#     r"\bpublic\s+(?:financial\s+)?management\b",
#     r"\bfiscal\s+(?:management|reform|policy)\b",
#     r"\bpfm\b",
#     r"\bpublic\s+sector\s+reform\b",
#     r"\binstitutional\s+reform\b",
#     r"\banti[-\s]?corruption\b",
#     r"\btransparency\b",
#     r"\baccountability\b",
#     # Strategy / Policy / Advisory
#     r"\bstrateg(?:y|ic|ies)\b",
#     r"\broadmap\b",
#     r"\baction\s+plan\b",
#     r"\bmaster\s+plan\b",
#     r"\bregulatory\s+(?:framework|reform|review)\b",
#     r"\bwhite\s+paper\b",
#     # Financial / Economic consulting
#     r"\bfinancial\s+(?:management|reform|advisory|analysis)\b",
#     r"\bgreen\s+finance\b",
#     r"\bdebt\s+management\b",
#     r"\bbudget\s+(?:reform|support)\b",
#     r"\btax\s+(?:policy|reform|administration)\b",
#     # Institutional / Capacity / Advisory support
#     r"\binstitutional\s+(?:support|strengthening|development)\b",
#     r"\borganizational\s+(?:development|review|assessment)\b",
#     r"\bstrengthening\b",
#     # Plans / Frameworks / Diagnostic (covers "Design a Biodiversity Finance Plan" etc.)
#     r"\b(?:adaptation|action|finance|development|recovery|resilience|investment|implementation)\s+plan\b",
#     r"\bnational\s+\w+\s+plan\b",
#     r"\b(?:develop(?:ment\s+of)?|design(?:ing)?|draft(?:ing)?|elaborat(?:ion|ing)|formulat(?:ion|ing)|compil(?:ing|ation\s+of)|prepar(?:ation\s+of|ing))\s+(?:a\s+|an\s+|the\s+)?\w[\w\s]{0,40}(?:plan|strategy|framework|policy|roadmap|guidelines?|protocol|methodology|report|assessment|review|study|analysis|manual|curriculum)\b",
#     r"\bframework\b",
#     r"\bbenchmarking\b",
#     r"\bdiagnostic\b",
#     r"\banalysis\b",
#     r"\bsupport\s+to\b",
#     r"\bdevelopment\s+of\b",
#     r"\bdesign\s+(?:of|and)\b",
#     r"\bdrafting\s+(?:of|a|an|the)\b",
#     r"\belaboration\b",
#     r"\bformulation\b",
#     r"\bcompilation\b",
#     # Digital / Transformation / Software / Platform (covers DCMS, web portals, dashboards)
#     r"\bdigital\s+(?:transformation|strategy|governance|economy|credential|registry|platform)\b",
#     r"\be[-\s]?government\b",
#     r"\binnovation\s+(?:strategy|advisory)\b",
#     r"\bwebsite\s+(?:development|design|redesign)\b",
#     r"\bweb\s+(?:portal|platform|application|app)\s+(?:development|design)\b",
#     r"\bplatform\s+development\b",
#     r"\bsystem\s+development\b",
#     r"\bsoftware\s+(?:development|solution|system)\b",
#     r"\bdashboard\s+(?:development|design)\b",
#     r"\bdatabase\s+(?:development|design|management\s+system)\b",
#     r"\bintegrated\s+\w[\w\s]{0,30}(?:system|platform|solution)\b",
#     r"\bdeployment\s+(?:and\s+\w+\s+)?(?:of\s+)?(?:a\s+)?(?:system|platform|solution)\b",
#     r"\boperationali[sz]ation\b",
#     r"\bdigital\s+solution\b",
#     r"\bICT\s+(?:solution|system|platform|development)\b",
#     # French
#     r"\b[eé]tude\b",
#     r"\bassistance\s+technique\b",
#     r"\bformation\s+(?:professionnelle|technique|en)\b",
#     r"\bréalisation\s+d(?:'|e\s+une)\s+[eé]tude\b",
#     r"\brecrutement\s+d(?:'|e\s+un)\b",
#     r"\bappel\s+[aà]\s+collaborateur\b",
#     r"\bprestation\s+intellectuelle\b",
#     r"\bconsultoría\b",
#     # Spanish
#     r"\bconsutor(?:ía|ia)\b",
#     r"\basistencia\s+técnica\b",
# ]




# # ─────────────────────────────────────────────────────────────────────────────
# #  NON-CONSULTING PATTERNS
# # ─────────────────────────────────────────────────────────────────────────────


# _NON_CONSULTING_PATTERNS: list[str] = [
#     # English
#     r"\bcatering\b",
#     r"\bevent\s+services?\b",
#     r"\blease\s+of\b",
#     r"\boffice\s+space\b",
#     r"\bcleaning\s+services?\b",
#     r"\bsecurity\s+(?:guard|services?)\b",
#     r"\bmaintenance\s+services?\b",
#     r"\bguarding\s+services?\b",
#     r"\bprinting\s+services?\b",
#     r"\bjanitorial\b",
#     r"\bwaste\s+collection\b",
#     r"\btransport(?:ation)?\s+services?\b",
#     r"\bcourier\s+services?\b",
#     r"\binsurance\s+(?:plans?|services?|coverage)\b",
#     r"\bstaff\s+retreat\b",
#     r"\bretreat\b",
#     r"\bconference\s+(?:services?|facilities)\b",
#     r"\bfacilitation\s+of\b",
#     r"\btravel\s+(?:management|agency|services?)\b",
#     r"\btranslation\s+services?\b",
#     r"\binterpreting\s+services?\b",
#     r"\bvenue\s+(?:hire|rental)\b",
#     r"\bcar\s+(?:hire|rental)\b",
#     r"\bfreight\s+forwarding\b",
#     r"\blogistics\s+services?\b",
#     # French
#     r"\blocation\s+de\b",
#     r"\bnettoyage\b",
#     r"\bgardiennage\b",
#     r"\brestauration\b",
#     r"\bh[eé]bergement\b",
#     r"\bmessagerie\b",
#     r"\btraduction\b",
#     r"\bimpression\s+de\b",
#     r"\bentretien\b",
#     r"\bservices?\s+de\s+maintenance\b",         # explicit phrase match
#     r"\bservices?\s+d['']entretien\b",  
#     # Spanish
#     r"\btransporte\s+de\b",
#     r"\blogística\b",
#     r"\balquiler\s+de\b",
#     r"\bmantenimiento\b",
#     r"\btraducci[oó]n\b",
#     r"\bimpresi[oó]n\s+de\b",
#     r"\brestauraci[oó]n\s+(?:de\s+)?(?:servicio|alimento)\b",
#     # Portuguese
#     r"\btranslação\b",
#     r"\bserviços\s+de\s+(?:limpeza|segurança|transporte|catering|tradução)\b",
# ]




# # ─────────────────────────────────────────────────────────────────────────────
# #  GOODS PATTERNS
# # ─────────────────────────────────────────────────────────────────────────────


# _GOODS_PATTERNS: list[str] = [
#     # English
#     r"\bsupply\s+(?:and\s+)?(?:delivery|of)\b",
#     r"\bdelivery\s+of\b",
#     r"\bprocurement\s+of\b",
#     r"\bpurchase\s+of\b",
#     r"\bsupply\s+of\b",
#     r"\bprovision\s+of\s+(?:equipment|goods|materials)\b",
#     r"\bsupplies\b",
#     r"\bequipment\b",
#     r"\bvehicles?\b",
#     r"\bfurniture\b",
#     r"\bcomputers?\b",
#     r"\blaptops?\b",
#     r"\bnotebooks?\b",
#     r"\bmedical\s+(?:supplies|equipment|devices?)\b",
#     r"\bpharmaceuticals?\b",
#     r"\bmedicines?\b",
#     r"\bmaterials?\b",
#     r"\btools?\b",
#     r"\bkits?\b",
#     r"\bPPE\b",
#     r"\buniforms?\b",
#     r"\bspare\s+parts?\b",
#     r"\bICT\s+equipment\b",
#     r"\boffice\s+supplies\b",
#     r"\bseeds?\b",
#     r"\bfertili[zs]ers?\b",
#     r"\bpesticides?\b",
#     r"\bagrochemicals?\b",
#     r"\bfuel\b",
#     r"\bgenerators?\b",
#     # French
#     r"\bachat\b",
#     r"\bfournitures?\b",
#     r"\bmatériels?\b",
#     r"\bacquisition\s+(?:de|d')\b",
#     r"\blivraison\b",
#     r"\bfornitura\b",                           # Italian/Portuguese tender term
#     # Spanish
#     r"\bsuministro\b",
#     r"\badquisici[oó]n\b",
#     r"\bcompra\s+de\b",
#     r"\binsumos\b",
#     r"\bequipos?\b",
#     r"\bmateriales\b",
#     r"\bsemillas\b",
#     r"\bfertilizantes\b",
#     r"\bbienes\b",
#     # Portuguese
#     r"\baquisição\b",
#     r"\bequipamentos?\b",
#     r"\bmateriais\b",
#     r"\bfornecimento\s+de\b",
#     r"\baquisição\s+de\b",
# ]




# # ─────────────────────────────────────────────────────────────────────────────
# #  EXTRACTION FUNCTIONS (importable)
# # ─────────────────────────────────────────────────────────────────────────────


# def extract_procurement_group(title: str) -> str:
#     """
#     Extract procurement group from a tender title.
#     Always returns a string — never None or null.
#     Returns one of: CONSULTING | WORKS | GOODS | NON-CONSULTING | Others
#     """
#     if not title or not title.strip():
#         return "Others"


#     lower = title.strip().lower()


#     for pat in _GOODS_INSTALL_PATTERNS:
#         if re.search(pat, lower, re.IGNORECASE):
#             return "GOODS"


#     for pat in _WORKS_PATTERNS:
#         if re.search(pat, lower, re.IGNORECASE):
#             return "WORKS"

#     # Check for NON-CONSULTING overrides BEFORE consulting patterns fire.
#     # This prevents "Consultancy Firm for Security Services" → CONSULTING.
#     for pat in _NON_CONSULTING_OVERRIDE_PATTERNS:
#         if re.search(pat, lower, re.IGNORECASE):
#             return "NON-CONSULTING"

#     for pat in _CONSULTING_PATTERNS:
#         if re.search(pat, lower, re.IGNORECASE):
#             return "CONSULTING"


#     for pat in _NON_CONSULTING_PATTERNS:
#         if re.search(pat, lower, re.IGNORECASE):
#             return "NON-CONSULTING"


#     for pat in _GOODS_PATTERNS:
#         if re.search(pat, lower, re.IGNORECASE):
#             return "GOODS"


#     return "Others"




# def get_procurement_group(
#     title:    str,
#     existing: Optional[str] = None,
#     portal:   Optional[str] = None,
# ) -> str:
#     """
#     Get procurement group with merge rules.
#     Always returns a string — never None or null.
#     If existing is already populated, keeps it (no overwrite).
#     """
#     if existing and existing.strip():
#         return existing
#     return extract_procurement_group(title)




# # ─────────────────────────────────────────────────────────────────────────────
# #  DATABASE UPDATE RUNNER
# # ─────────────────────────────────────────────────────────────────────────────


# def run_update(
#     mode:    str = "nulls-and-others",
#     dry_run: bool = False,
#     limit:   int | None = None,
# ) -> None:
#     """
#     Update procurement_group on enriched_tenders rows.
#     Runs independently — does not touch enrichment_status.
#     """
#     try:
#         from db import SessionLocal
#         from sqlalchemy import text
#     except ImportError as e:
#         log.error("Import error (db): %s", e)
#         return


#     if mode == "all":
#         where = ""
#     elif mode == "nulls-only":
#         where = "WHERE e.procurement_group IS NULL"
#     elif mode == "others-only":
#         where = "WHERE e.procurement_group = 'Others'"
#     else:
#         where = "WHERE e.procurement_group IS NULL OR e.procurement_group = 'Others'"


#     limit_clause = f"LIMIT {limit}" if limit else ""


#     select_sql = f"""
#         SELECT e.id, t.title, e.procurement_group
#         FROM enriched_tenders e
#         JOIN tenders t ON e.tender_id = t.id
#         {where}
#         ORDER BY e.id
#         {limit_clause}
#     """


#     total    = 0
#     counters = dict(updated=0, failed=0)


#     session = SessionLocal()
#     try:
#         rows  = session.execute(text(select_sql)).mappings().all()
#         total = len(rows)
#         log.info("Found %d rows to process (mode: %s)", total, mode)


#         if total == 0:
#             log.info("Nothing to do.")
#             return


#         batch_params = []


#         for row in rows:
#             new_group = extract_procurement_group(row["title"] or "")


#             if dry_run:
#                 log.info(
#                     "  [DRY-RUN] id=%-6s  old=%-20s  new=%-20s  title=%s",
#                     row["id"],
#                     row["procurement_group"] or "NULL",
#                     new_group,
#                     (row["title"] or "")[:80],
#                 )
#                 counters["updated"] += 1
#                 continue


#             batch_params.append({"procurement_group": new_group, "id": row["id"]})


#             if len(batch_params) >= BATCH_SIZE:
#                 try:
#                     session.execute(
#                         text("UPDATE enriched_tenders SET procurement_group = :procurement_group WHERE id = :id"),
#                         batch_params,
#                     )
#                     session.commit()
#                     counters["updated"] += len(batch_params)
#                     log.info("  ... committed %d rows", len(batch_params))
#                 except Exception as e:
#                     session.rollback()
#                     log.error("  ✗ Batch failed: %s — %s", type(e).__name__, str(e))
#                     counters["failed"] += len(batch_params)
#                 batch_params = []


#         if batch_params:
#             try:
#                 session.execute(
#                     text("UPDATE enriched_tenders SET procurement_group = :procurement_group WHERE id = :id"),
#                     batch_params,
#                 )
#                 session.commit()
#                 counters["updated"] += len(batch_params)
#                 log.info("  ... committed final %d rows", len(batch_params))
#             except Exception as e:
#                 session.rollback()
#                 log.error("  ✗ Final batch failed: %s — %s", type(e).__name__, str(e))
#                 counters["failed"] += len(batch_params)


#     except Exception as e:
#         session.rollback()
#         log.error("Fatal error: %s — %s", type(e).__name__, str(e), exc_info=True)
#     finally:
#         session.close()


#     log.info(
#         "Done — total=%d  updated=%d  failed=%d%s",
#         total, counters["updated"], counters["failed"],
#         "  [DRY-RUN]" if dry_run else "",
#     )




# # ─────────────────────────────────────────────────────────────────────────────
# #  CLI
# # ─────────────────────────────────────────────────────────────────────────────


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Extract and update procurement_group on enriched_tenders."
#     )


#     mode_group = parser.add_mutually_exclusive_group()
#     mode_group.add_argument("--all", dest="mode", action="store_const", const="all",
#         help="Re-extract and update every row.")
#     mode_group.add_argument("--nulls-only", dest="mode", action="store_const", const="nulls-only",
#         help="Only fill rows where procurement_group IS NULL.")
#     mode_group.add_argument("--others-only", dest="mode", action="store_const", const="others-only",
#         help="Only update rows where procurement_group = 'Others'.")
#     mode_group.add_argument("--nulls-and-others", dest="mode", action="store_const", const="nulls-and-others",
#         help="Fill NULL and 'Others' rows (default).")
#     parser.set_defaults(mode="nulls-and-others")


#     parser.add_argument("--dry-run", action="store_true",
#         help="Print what would be updated without writing to DB.")
#     parser.add_argument("--limit", type=int, default=None, metavar="N",
#         help="Process at most N rows.")


#     args = parser.parse_args()
#     run_update(mode=args.mode, dry_run=args.dry_run, limit=args.limit)



"""
enricher/procurement_group.py
==============================
Standalone procurement group extraction from tender titles.
Determines procurement group for ALL portals.
Fills gaps where procurement_group is NULL or 'Others'.

Groups:
    CONSULTING      — intellectual services: studies, assessments,
                      consultancies, evaluations, technical assistance
    WORKS           — physical construction, renovation, installation
    GOODS           — supply and delivery of physical items
    NON-CONSULTING  — operational services: catering, cleaning,
                      security, lease, events
    Others          — could not be determined from title

Architecture:
    Layer 1 — keyword scoring (STOP / HINT / NLP decision)
              Fast, no model load required.
              If confident (STOP), skips NLP entirely.
    Layer 2 — NLP embedding similarity
              paraphrase-multilingual-MiniLM-L12-v2
              Only runs when Layer 1 is not confident.

Rules:
    - NEVER returns None or null — always returns a string
    - If existing value is already populated, returns existing (no overwrite)
    - If nothing matches, returns "Others"

Usage (import):
    from procurement_group import extract_procurement_group
    group = extract_procurement_group(title)

    from procurement_group import get_procurement_group
    group = get_procurement_group(title, existing)

Run directly to update the database:
    python enricher/procurement_group.py                      # fill NULL and 'Others' (default)
    python enricher/procurement_group.py --nulls-only         # only NULL rows
    python enricher/procurement_group.py --others-only        # only 'Others' rows
    python enricher/procurement_group.py --all                # re-extract every row
    python enricher/procurement_group.py --dry-run            # print only, no DB writes
    python enricher/procurement_group.py --limit 100          # first 100 rows only
    python enricher/procurement_group.py --self-test          # run known error test suite
"""

import argparse
import csv
import logging
import os
import re
import sys
from collections import defaultdict
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

# =============================================================================
# LAYER 1 — THRESHOLDS & WEIGHTS
# =============================================================================

THRESHOLD_STOP = 70
THRESHOLD_HINT = 40

W_STOP = 100
W_HINT =  40

# =============================================================================
# LAYER 1 — KEYWORD LISTS
# =============================================================================

# ── CONSULTING ────────────────────────────────────────────────────────────────
CONSULTING_STOP = [
    "capacity building", "technical assistance", "individual consultant",
    "national consultant", "international consultant", "consultancy firm",
    "feasibility study", "impact assessment", "baseline assessment",
    "mid-term evaluation", "research analyst", "external collaborator",
    "legal advisor", "policy specialist", "gender equality",
    "environmental policy", "social security standards",
    "biodiversity finance", "blended finance", "climate risk finance",
    "consultancy services", "consulting services",
    "supervision of construction",
    "design review",
    "call for external collaborator",
    # Civil-works consulting
    "preparation of technical documentation",
    "preparation of design",
    "technical supervision",
    "supervision of works",
    "architectural survey",
    "design and supervision",
    # bare "consultancy"
    "consultancy",
    # Pilot / demand / market studies
    "pilot study",
    "market assessment",
    "demand assessment",
    "market and demand assessment",
    "market & demand assessment",
    # Training programme as a service
    "training program",
    "training programme",
    "delivery of training",
    # Spanish strategy / roadmap / technology package terms
    "hoja de ruta",
    "paquetes tecnológicos",
    "paquetes tecnologicos",
    "elaboración de",
    "elaboracion de",
    "producción de paquetes",
    "produccion de paquetes",
    # Individual expert role titles
    "structural engineer",
    "project engineer",
    "finance specialist",
    "procurement specialist",
    "monitoring specialist",
    "evaluation specialist",
    "project manager consultant",
    "team leader consultant",
    # Scientific / analytical services
    "secuenciación",
    "secuenciacion",
    "rna-seq",
    "extracción de arn",
    "extraccion de arn",
    "servicio de extracción",
    "servicio de extraccion",
    "análisis de datos",
    "analisis de datos",
    "sequencing service",
    # End-of-term / final review phrases
    "end-of-term review",
    "end of term review",
    "final review",
    "terminal evaluation",
    "project completion review",
    # Value chain / sourcing analysis
    "value chain analysis",
    "value chain assessment",
    "sourcing strategy",
    "sourcing in the",
    # Development of a plan/roadmap/strategy
    "development of a plan",
    "development of a national plan",
    "development of a roadmap",
    "development of a strategy",
    "development of a framework",
    "development of a disaster risk",
    "development of a climate",
    "rfq-development of",
    "rfp-development of",
    # French
    "assistance technique", "cabinet pour", "consultant individuel",
    "recrutement d'un consultant", "recrutement d'un cabinet",
    "recrutement d'un expert", "recrutement d'une firme",
    "sélection d'un consultant", "sélection d'un expert",
    "sélection d'un cabinet",
    "appel à candidature", "appel à manifestation",
    "rapport d'achèvement", "étude de faisabilité",
    "bureau d'études",
    "maître d'œuvre",
    # Spanish/Portuguese
    "consultoría", "asistencia técnica", "consultor individual",
]

CONSULTING_HINT = [
    "evaluation", "assessment", "analysis", "research", "review",
    "specialist", "advisor", "adviser", "expert", "audit", "mapping",
    "survey", "study", "policy", "capacity", "training", "workshop",
    "monitoring", "strategy", "coaching", "mentoring", "facilitation",
    "baseline", "endline", "governance", "strengthening", "advisory",
    "institutional", "diagnostic", "framework", "roadmap",
    # French
    "étude", "évaluation", "recrutement", "formation", "spécialiste",
    "cabinet", "firme", "prestataire",
    # Spanish
    "evaluación", "asesoría",
]

# ── WORKS ─────────────────────────────────────────────────────────────────────
WORKS_STOP = [
    # French
    "travaux aménagement réhabilitation", "travaux aménagement",
    "travaux construction", "travaux réalisation", "aménagement réhabilitation",
    "travaux de réhabilitation", "travaux de construction",
    "génie civil",
    # English
    "construction rehabilitation", "civil works", "construction of",
    "rehabilitation of", "fit-out contract", "upgrade works",
    "maintenance works", "latrine construction", "borehole drilling",
    "road construction", "bridge construction",
    # supply+install combinations → WORKS not GOODS
    "supply and installation of",
    "supply, delivery and installation of",
    "supply, installation and commissioning",
    "supply and install",
    "fourniture et installation",
    "acquisition et installation",
    "achat et installation",
    "suministro e instalación",
    "suministro e instalacion",
    "suministro, instalación",
    "fornecimento e instalação",
    "delivery and installation of",
    "delivery, installation",
    "installation and commissioning of",
    # Conservation / improvement of facilities
    "conservation works",
    "improvement of functionality",
    "improvement of indoor",
    "refurbishment of",
    "renovation of",
    # production and installation
    "production and installation of",
    "production et installation",
    # Truck/vehicle repair
    "reparación integral",
    "reparacion integral",
    "repair and rehabilitation of",
    "overhaul of",
    # Greenhouse / agricultural structure installation
    "installation de serres",
    "installation of greenhouses",
    "installation of irrigation",
    # Spanish/Portuguese
    "construcción de", "rehabilitación de", "obras civiles",
]

WORKS_HINT = [
    "construction", "rehabilitation", "renovation", "works", "refurbishment",
    "drilling", "borehole", "excavation", "paving", "fencing", "demolition",
    "installation of", "plumbing", "infrastructure",
    # French
    "travaux", "réhabilitation", "aménagement", "réalisation", "réfection",
    "voirie", "assainissement", "forages", "adduction d'eau",
    # Spanish/Portuguese
    "construcción", "rehabilitación", "obras", "perforación", "saneamento",
]

# ── GOODS ─────────────────────────────────────────────────────────────────────
GOODS_STOP = [
    "supply and delivery of",
    "supply delivery",
    "supply of",
    "delivery of",
    "procurement of",
    "purchase of",
    "procurement and delivery",
    "medical equipment",
    "stationery items",
    "production lines",
    # Furniture / tools
    "furniture for",
    "office furniture",
    "mobilier de bureau",
    "equipos y herramientas",
    "tools and equipment",
    "herramientas del almacén",
    "herramientas del almacen",
    # Fuel supply
    "fourniture de carburant",
    "supply of fuel",
    "supply of diesel",
    "suministro de combustible",
    "combustible ulsd",
    # Security cameras / access control
    "cámaras de seguridad",
    "camaras de seguridad",
    "security cameras",
    "controles de acceso",
    "access control equipment",
    # Vehicle purchase
    "adquisición de vehículos",
    "adquisicion de vehiculos",
    "purchase of vehicles",
    "procurement of vehicles",
    # Software AND equipment together
    "software and equipment",
    "software y equipos",
    "equipment for waste management",
    # French — pure supply
    "fourniture et livraison",
    "fourniture et pose",
    "acquisition de",
    "achat de",
    # Spanish/Portuguese — pure supply
    "suministro de",
    "adquisición de",
    "aquisição de",
    "fornecimento de",
]

GOODS_HINT = [
    "supply", "delivery", "procurement", "purchase", "equipment",
    "vehicle", "furniture", "medicine", "laptop", "computer",
    "material", "consumable", "seed", "fertilizer", "fuel", "kit",
    "tank", "generator", "spare parts", "ict equipment", "ppe",
    "uniform", "pharmaceutical", "reagent",
    # French
    "fourniture", "matériel", "acquisition", "livraison", "achat",
    "semences", "intrants", "véhicule",
    # Spanish/Portuguese
    "suministro", "adquisición", "equipos", "materiales",
    "semillas", "fertilizantes", "bienes",
]

# ── NON-CONSULTING ────────────────────────────────────────────────────────────
NON_CONSULTING_STOP = [
    "events management", "event management services",
    "catering services", "hotel accommodation", "cleaning services",
    "canteen services", "maintenance services", "translation services",
    "vehicle hire", "car hire", "vehicle rental",
    "event planning services", "provision of hotel", "hotel services",
    "air tickets", "charter flight", "internet access services",
    "fiber internet connection", "media production services",
    "videography services", "lta translators", "roster of interpreters",
    "security guarding", "janitorial services", "freight forwarding",
    "travel management", "conference package", "staff retreat",
    "printing services", "courier services", "waste collection",
    # Printing/impression (French)
    "impression des",
    "impression de",
    "impression et",
    # Team building / retreat
    "team building",
    "team retreat",
    "team building support",
    # Office rent / long-term rent
    "office rent",
    "long term office rent",
    "long-term office rent",
    "location de bureau",
    "loyer bureau",
    # Cleaning & gardening
    "cleaning & gardening",
    "cleaning and gardening services",
    "cleaning services on lta",
    "lta for cleaning services",
    # Transportation/distribution services
    "transportation services for",
    "transport services for",
    "services for distribution of",
    "distribution services for",
    # Insurance
    "insurance coverage",
    "insurance plan",
    "insurance services",
    "group medical insurance",
    "property insurance",
    "vehicle insurance",
    "póliza de seguros",
    "póliza de seguro",
    "contratación de póliza",
    "seguro de vehículos",
    "seguro vehicular",
    "assurance véhicules",
    "contrat d'assurance",
    "couverture d'assurance",
    # Cleaning
    "cleaning and gardening",
    "nettoyage et jardinage",
    "limpieza y fumigación",
    "servicios de limpieza",
    "servicio de limpieza",
    "lta cleaning",
    "lta for cleaning",
    # Car rental / taxi
    "car rental services",
    "taxi services",
    "provision of car rental",
    "provision of vehicle rental",
    "location de véhicule",
    "location de voitures",
    "servicios de transporte fluvial",
    # SaaS / licences
    "software license subscription",
    "software licence",
    "saas subscription",
    "cisco smart maintenance",
    "microsoft enterprise agreement",
    # French
    "service de maintenance", "restauration jours",
    "location salle restauration", "accord long terme transport",
    "nettoyage", "gardiennage", "messagerie", "hébergement",
    # Spanish
    "servicio agencia empleo", "servicio agencia",
    "servicios de catering", "servicios de limpieza",
]

NON_CONSULTING_HINT = [
    "hotel", "catering", "accommodation", "venue", "event", "events",
    "conference", "transport", "maintenance", "cleaning",
    "translation", "canteen", "printing", "insurance", "charter",
    "hire", "rental", "videography", "media production",
    "security guard", "janitorial", "courier", "waste",
    "póliza", "seguro", "assurance", "nettoyage", "gardiennage",
    "subscription", "licence", "license",
    # French
    "restauration", "entretien", "traduction", "impression",
    # Spanish
    "servicio", "agencia", "mantenimiento", "traducción",
    "alquiler", "logística",
]


# =============================================================================
# LAYER 1 — PRE-CHECKS  (fire before keyword scoring)
# =============================================================================

# Supply+install → WORKS
_SUPPLY_INSTALL_WORKS = re.compile(
    r"""
    \b(
        supply[\s,]+(?:and\s+)?(?:delivery[\s,]+(?:and\s+)?)?install(?:ation|ling)?
      | delivery[\s,]+(?:and\s+)?install(?:ation|ing)?
      | fourniture\s+et\s+(?:pose|installation)
      | acquisition\s+et\s+installation
      | achat\s+et\s+installation
      | suministro\s+e\s+instalaci[oó]n
      | fornecimento\s+e\s+instala[cç][aã]o
      | installation\s+and\s+commissioning
      | supply[,\s]+install(?:ation|ling)?
      | production\s+and\s+installation\s+of
      | production\s+et\s+installation\s+de
      | installation\s+de\s+serres
      | installation\s+of\s+(?:greenhouses?|irrigation|solar)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _supply_install_is_works(text: str) -> bool:
    return bool(_SUPPLY_INSTALL_WORKS.search(text))


# French "consultation pour la fourniture" → GOODS (RFP process name, not consulting)
_CONSULTATION_POUR_FOURNITURE = re.compile(
    r"""
    \bconsultation\s+pour\s+
    (?:la\s+)?
    (?:fourniture | l['']achat | l['']acquisition | livraison | achat)
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _consultation_pour_fourniture(text: str) -> bool:
    return bool(_CONSULTATION_POUR_FOURNITURE.search(text))


# Hard NON-CONSULTING anchors
_NON_CONSULTING_ANCHORS = re.compile(
    r"""
    \b(
        p[oó]liza\s+de\s+seguro
      | contrataci[oó]n\s+de\s+p[oó]liza
      | seguro\s+de\s+veh[ií]culos?
      | seguro\s+vehicular
      | seguros?\s+veh[ií]culos?
      | assurance\s+v[eé]hicules?
      | contrat\s+d['']assurance
      | couverture\s+d['']assurance
      | group\s+medical\s+insurance
      | insurance\s+(?:coverage|plan|services?)
      | vehicle\s+(?:hire|rental)
      | car\s+(?:hire|rental)\s+services?
      | taxi\s+services?
      | provision\s+of\s+(?:car|vehicle)\s+rental
      | location\s+de\s+v[eé]hicules?
      | location\s+de\s+voitures?
      | (?:cisco\s+smart|microsoft\s+enterprise|citrix)\s+
        (?:maintenance|subscription|agreement|license|licence)
      | software\s+(?:license|licence)\s+subscription
      | saas\s+(?:tool|platform|subscription)
      | nettoyage\s+et\s+jardinage
      | limpieza\s+y\s+fumigaci[oó]n
      | servicios?\s+de\s+limpieza
      | impression\s+des?\b
      | impression\s+et\b
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _non_consulting_anchor(text: str) -> bool:
    return bool(_NON_CONSULTING_ANCHORS.search(text))


# Strong procurement-verb anchor → GOODS
_GOODS_PROCUREMENT_STARTS = re.compile(
    r"""
    ^(
        adquisici[oó]n\s+de
      | achat\s+de
      | acquisition\s+de
      | suministro\s+de
      | suministro\s+y\s+entrega
      | fourniture\s+et\s+livraison
      | aquisição\s+de
      | fornecimento\s+de
      | procurement\s+of
      | purchase\s+of
      | supply\s+and\s+delivery\s+of
      | supply\s+of
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)
_GOODS_ANCHOR_EXCLUDE = re.compile(
    r"\b(install|service|servicios?|consultant|advisory|supervision|design|assessment|evaluation)\b",
    re.IGNORECASE,
)

def _goods_procurement_anchor(text: str) -> bool:
    if _GOODS_PROCUREMENT_STARTS.match(text.strip()):
        return not bool(_GOODS_ANCHOR_EXCLUDE.search(text))
    return False


# Hard furniture / tools anchor → GOODS
_GOODS_HARD_ANCHOR = re.compile(
    r"""
    \b(
        furniture\s+for
      | office\s+furniture
      | mobilier\s+(?:de\s+bureau|du\s+bureau)
      | equipos\s+y\s+herramientas
      | herramientas\s+del\s+almac[eé]n
      | tools\s+and\s+equipment
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _goods_hard_anchor(text: str) -> bool:
    return bool(_GOODS_HARD_ANCHOR.search(text))


# Conservation / facility improvement → WORKS
_WORKS_HARD_ANCHOR = re.compile(
    r"""
    \b(
        conservation\s+works?
      | improvement\s+of\s+(?:functionality|indoor|outdoor|the\s+(?:functionality|indoor|outdoor))
      | refurbishment\s+of
      | renovation\s+of\s+(?:the\s+)?(?:building|facility|facilities|centre|center|site)
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _works_hard_anchor(text: str) -> bool:
    return bool(_WORKS_HARD_ANCHOR.search(text))


# IC- prefix → Individual Consultant → CONSULTING
_IC_PREFIX = re.compile(
    r"(?:^|(?<=\s)|(?<=_))\bIC\s*[-–—:]\s*\w",
    re.IGNORECASE,
)

def _is_ic_prefix(text: str) -> bool:
    return bool(_IC_PREFIX.search(text))


# consultant(e) / consultor(a) gendered forms → CONSULTING
_CONSULTANT_GENDERED = re.compile(
    r"\bconsultan(?:t(?:\(e\)|e)?|t(?:e)?)\b"
    r"|\bconsultor(?:\(a\)|a)?\b"
    r"|\bconsultora?\b",
    re.IGNORECASE,
)

def _is_gendered_consultant(text: str) -> bool:
    return bool(_CONSULTANT_GENDERED.search(text))


# Prequalification invitation → GOODS
_PREQUALIFICATION_GOODS = re.compile(
    r"\b(?:invitation\s+for\s+prequalification|prequalification\s+of\s+(?:suppliers?|manufacturers?))\b",
    re.IGNORECASE,
)

def _is_prequalification_goods(text: str) -> bool:
    return bool(_PREQUALIFICATION_GOODS.search(text))


# Supply of machinery/tractors → suppress WORKS (these are GOODS)
_MACHINERY_SUPPLY = re.compile(
    r"\bsupply\s+of\b.{0,40}\b(tractors?|machineries?|machinery|harvesters?|combines?)\b",
    re.IGNORECASE,
)


# =============================================================================
# LAYER 1 — CONSULTING OVERRIDE
# Suppresses WORKS score when a strong consulting signal is present
# =============================================================================
_CONSULTING_OVERRIDE = [
    r"\bconsultancy\b",
    r"\bconsultancy\s+services?\b",
    r"\bconsulting\s+services?\b",
    r"\btechnical\s+assistance\b",
    r"\bassistance\s+technique\b",
    r"\brecrutement\s+d['\s]un\b",
    r"\brecrutement\s+d['\s]une\b",
    r"\bsélection\s+d['\s]un\b",
    r"\bindividual\s+consultant\b",
    r"\bconsultant\s+individuel\b",
    r"\bfeasibility\s+study\b",
    r"\bsupervision\s+of\s+(?:construction|works?|travaux)\b",
    r"\bdesign\s+(?:review|and\s+supervision)\b",
    r"\bcall\s+for\s+(?:external\s+)?collaborat\w+\b",
    r"\bappel\s+[aà]\s+(?:candidature|manifestation)\b",
    r"\b(?:mid[-\s]?term|final|impact)\s+evaluation\b",
    r"\bend[-\s]of[-\s]term\s+review\b",
    r"\bterminal\s+evaluation\b",
    r"\bIC\s*[-–—:]\s*\w",
    r"\bconsultan(?:t(?:\(e\)|e)?|t(?:e)?)\b",
    r"\bconsultor(?:\(a\)|a)?\b",
    r"\bsupervision\s+of\s+works?\b",
    r"\btechnical\s+supervision\b",
    r"\bsupervision\s+(?:des?|for)\s+(?:the\s+)?(?:construction|rehabilitation|works?|travaux)\b",
    r"\bpreparation\s+of\s+(?:technical\s+)?(?:design|documentation|drawings?)\b",
    r"\bdesign\s+(?:and\s+)?(?:prepare|develop|preparation|development)\b",
    r"\barchitectural\s+(?:survey|design|services?)\b",
    r"\bmaître\s+d['\s]œuvre\b",
    r"\bbur(?:eau|eaux)\s+d['\s](?:étude|études)\b",
    r"\btechni(?:cal|que)\s+(?:design|documentation|drawings?)\b",
    r"\bservicio\s+de\s+extracci[oó]n\b",
    r"\bsecuenciaci[oó]n\b",
    r"\brna-seq\b",
]

def _has_consulting_override(text: str) -> bool:
    for pat in _CONSULTING_OVERRIDE:
        if re.search(pat, text, re.IGNORECASE):
            return True
    return False


# =============================================================================
# LAYER 1 — BUILD KEYWORD SCORING DICT
# =============================================================================

def _build_kw_dict() -> dict:
    groups = {
        "CONSULTING":     (CONSULTING_STOP,     CONSULTING_HINT),
        "WORKS":          (WORKS_STOP,           WORKS_HINT),
        "GOODS":          (GOODS_STOP,           GOODS_HINT),
        "NON-CONSULTING": (NON_CONSULTING_STOP,  NON_CONSULTING_HINT),
    }
    result = {}
    for group, (stops, hints) in groups.items():
        kws = {}
        for phrase in stops:
            kws[phrase.lower()] = W_STOP
        for phrase in hints:
            if phrase.lower() not in kws:
                kws[phrase.lower()] = W_HINT
        result[group] = kws
    return result

_KW_DICT = _build_kw_dict()


# =============================================================================
# LAYER 1 — CLASSIFY
# =============================================================================

def _layer1_classify(title: str) -> dict:
    """
    Returns winner, confidence, decision (STOP/HINT/NLP), matched_keywords.
    Decision STOP means Layer 1 is confident — skip NLP.
    """
    if not title or not title.strip():
        return {"winner": "Others", "confidence": 0.0,
                "decision": "NLP", "matched_keywords": ""}

    text = title.strip().lower()
    scores: dict = defaultdict(float)
    matched = []

    # ── PRE-CHECKS ────────────────────────────────────────────────────────────

    if _is_prequalification_goods(text):
        scores["GOODS"] += W_STOP * 1.5
        matched.append(("prequalification→GOODS", "GOODS", W_STOP * 1.5))

    elif _consultation_pour_fourniture(text):
        scores["GOODS"] += W_STOP * 1.5
        matched.append(("consultation_pour_fourniture→GOODS", "GOODS", W_STOP * 1.5))

    elif _goods_hard_anchor(text):
        scores["GOODS"] += W_STOP * 1.5
        matched.append(("goods_hard_anchor", "GOODS", W_STOP * 1.5))

    elif _works_hard_anchor(text):
        scores["WORKS"] += W_STOP * 1.5
        matched.append(("works_hard_anchor", "WORKS", W_STOP * 1.5))

    elif _is_ic_prefix(title):
        scores["CONSULTING"] += W_STOP * 1.5
        matched.append(("IC_prefix→CONSULTING", "CONSULTING", W_STOP * 1.5))

    elif _is_gendered_consultant(text):
        scores["CONSULTING"] += W_STOP * 1.4
        matched.append(("gendered_consultant→CONSULTING", "CONSULTING", W_STOP * 1.4))

    # These run regardless (can stack with above)
    if _supply_install_is_works(text):
        scores["WORKS"] += W_STOP * 1.5
        matched.append(("supply+install→WORKS", "WORKS", W_STOP * 1.5))

    if _non_consulting_anchor(text):
        scores["NON-CONSULTING"] += W_STOP * 1.5
        matched.append(("non_consulting_anchor", "NON-CONSULTING", W_STOP * 1.5))

    if _goods_procurement_anchor(text):
        scores["GOODS"] += W_STOP * 1.2
        matched.append(("goods_procurement_anchor", "GOODS", W_STOP * 1.2))

    # ── KEYWORD SCORING ───────────────────────────────────────────────────────
    for group, keywords in _KW_DICT.items():
        for phrase, weight in keywords.items():
            if " " in phrase:
                if phrase in text:
                    scores[group] += weight
                    matched.append((phrase, group, weight))
            else:
                pat = r"\b" + re.escape(phrase) + r"\b"
                if re.search(pat, text, re.IGNORECASE):
                    scores[group] += weight
                    matched.append((phrase, group, weight))

    if not scores:
        return {"winner": "Others", "confidence": 0.0,
                "decision": "NLP", "matched_keywords": ""}

    # ── POST-SCORING SUPPRESSION ──────────────────────────────────────────────

    # Suppress WORKS when consulting signal present
    if scores.get("CONSULTING", 0) > 0 and _has_consulting_override(title):
        if "WORKS" in scores:
            scores["WORKS"] *= 0.20

    # Suppress GOODS when supply+install fired
    if scores.get("WORKS", 0) > W_STOP and _supply_install_is_works(text):
        if "GOODS" in scores:
            scores["GOODS"] *= 0.30

    # Suppress WORKS when supply of machinery/tractors (those are GOODS)
    if _MACHINERY_SUPPLY.search(text) and scores.get("GOODS", 0) > 0:
        if "WORKS" in scores:
            scores["WORKS"] *= 0.20

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    winner        = sorted_scores[0][0]
    top_score     = sorted_scores[0][1]
    second_score  = sorted_scores[1][1] if len(sorted_scores) > 1 else 0.0

    total    = sum(scores.values())
    raw_conf = (top_score / total) * 100 if total > 0 else 0.0

    gap = top_score - second_score
    if gap < 15:   raw_conf *= 0.65
    elif gap < 30: raw_conf *= 0.82

    if top_score <= W_HINT * 1.5:
        raw_conf = min(raw_conf, 48.0)

    confidence = min(raw_conf, 99.0)

    if confidence >= THRESHOLD_STOP:
        decision = "STOP"
    elif confidence >= THRESHOLD_HINT:
        decision = "HINT"
    else:
        decision = "NLP"

    top_matched = sorted(
        [m for m in matched if m[1] == winner],
        key=lambda x: x[2], reverse=True
    )[:3]

    return {
        "winner":           winner,
        "confidence":       round(confidence, 1),
        "decision":         decision,
        "matched_keywords": ", ".join(f"{m[0]}({m[2]:.0f})" for m in top_matched),
    }


# =============================================================================
# LAYER 2 — NLP MODEL
# =============================================================================

PROCUREMENT_NLP_GROUPS = [
    {
        "name": "CONSULTING",
        "description": (
            "consulting advisory technical assistance expert services "
            "feasibility study assessment evaluation audit research "
            "capacity building training policy development institutional support "
            "governance public financial management strategy monitoring and evaluation"
        ),
        "prototypes": [
            "consultancy services for technical assistance and advisory support",
            "recruitment of an individual consultant or expert advisor",
            "feasibility study and engineering design services",
            "mid-term evaluation and impact assessment consultancy",
            "financial audit and accounting review services",
            "capacity building and training programme for institutions",
            "recrutement d un consultant individuel ou d un bureau d etudes",
            "assistance technique pour la gouvernance et la gestion financiere",
            "selection d un expert international en appui institutionnel",
            "supervision of construction works by consulting firm",
            "technical supervision and design review for rehabilitation works",
            "preparation of technical documentation and design drawings",
            "architectural survey and structural assessment consultancy",
            "policy reform advisory and public sector institutional strengthening",
            "monitoring and evaluation specialist recruitment",
            "call for external collaborator or individual expert",
            "pilot study on payment system usage and demand assessment",
            "market and demand assessment for financial products and services",
            "sourcing in recycling value chain assessment and strategy",
            "end-of-term review and development of national strategic plan",
            "rna sequencing and total rna extraction analytical service",
            "elaboracion de hoja de ruta para sector industrial consultoria",
            "produccion de paquetes tecnologicos para sector agricola",
        ],
    },
    {
        "name": "WORKS",
        "description": (
            "physical construction rehabilitation civil works building road works "
            "bridge construction water supply infrastructure sanitation works "
            "irrigation scheme physical installation renovation drilling boreholes "
            "dam construction power plant electrification physical infrastructure "
            "supply and installation of equipment commissioning "
            "conservation of heritage sites improvement of sport facilities"
        ),
        "prototypes": [
            "construction of public sanitation facilities and latrines",
            "rehabilitation of roads and bridges civil works contract",
            "travaux de construction d un batiment ou d une infrastructure physique",
            "drilling of boreholes and water supply network installation",
            "road construction and rehabilitation of transport infrastructure",
            "construction of health centre and school buildings",
            "civil works for water treatment plant and dam construction",
            "supply and installation of solar photovoltaic system and commissioning",
            "supply delivery and installation of medical equipment in health facility",
            "fourniture et installation de panneaux solaires et mise en service",
            "supply installation and commissioning of water pumping station",
            "conservation works at heritage site restoration",
            "improvement of functionality of indoor sport facilities in schools",
            "refurbishment of office building and renovation of infrastructure",
        ],
    },
    {
        "name": "GOODS",
        "description": (
            "supply and delivery of physical goods procurement of equipment "
            "purchase of medical supplies pharmaceutical products agricultural inputs "
            "vehicles furniture ICT hardware seeds fertilisers materials commodities "
            "drug prequalification pharmaceutical procurement invitation"
        ),
        "prototypes": [
            "supply and delivery of medical equipment and pharmaceuticals",
            "procurement and delivery of vehicles and office furniture",
            "acquisition de materiels informatiques et de mobiliers de bureau",
            "fourniture et livraison de semences et d intrants agricoles",
            "supply of ICT equipment computers and laptops",
            "purchase of generators and electrical equipment",
            "suministro y entrega de equipos y materiales",
            "procurement of agricultural inputs seeds and fertilisers",
            "supply and delivery of food commodities and nutrition products",
            "procurement of medicines vaccines and medical consumables",
            "furniture for training centre modular office equipment supply",
            "invitation for prequalification of antiretroviral drug",
            "equipos y herramientas del almacen del instituto tecnologico",
        ],
    },
    {
        "name": "NON-CONSULTING",
        "description": (
            "catering food services cleaning janitorial security guarding "
            "hotel accommodation booking conference venue hire "
            "freight forwarding vehicle hire car rental taxi services "
            "printing services interpretation translation "
            "building maintenance insurance coverage policy "
            "waste collection courier postal media production event organisation "
            "travel management air tickets accommodation software subscription licence"
        ),
        "prototypes": [
            "provision of catering food and hotel accommodation services",
            "security guarding and office cleaning janitorial services",
            "conference package venue hire and event management services",
            "freight forwarding customs clearing and cargo transport services",
            "vehicle hire and car rental and taxi services for staff transport",
            "printing and document reproduction services for publications",
            "translation and interpretation services for conferences and meetings",
            "waste collection removal and disposal services for premises",
            "services de nettoyage et de gardiennage des locaux du bureau",
            "media production photography videography services",
            "hotel accommodation air ticket and travel management services",
            "provision of insurance coverage for vehicles and office assets",
            "software licence subscription and enterprise support services",
            "impression des outils de collecte des donnees et supports",
            "team building support and staff retreat organisation",
            "transportation services for distribution of health products",
        ],
    },
]

_nlp_model      = None
_nlp_embeddings = None


def _load_nlp_model():
    global _nlp_model, _nlp_embeddings
    if _nlp_model is not None:
        return _nlp_model, _nlp_embeddings

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except ImportError:
        log.warning("sentence-transformers not installed — NLP layer disabled.")
        log.warning("pip install sentence-transformers --break-system-packages")
        return None, None

    model_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    log.info("Loading NLP model: %s", model_name)
    _nlp_model = SentenceTransformer(model_name, device="cpu")

    import numpy as np
    group_embeddings = {}
    for g in PROCUREMENT_NLP_GROUPS:
        sentences = [g["description"]] + g["prototypes"]
        embs = _nlp_model.encode(sentences, normalize_embeddings=True)
        group_embeddings[g["name"]] = np.mean(embs, axis=0)

    _nlp_embeddings = group_embeddings
    log.info("NLP model ready.")
    return _nlp_model, _nlp_embeddings


def _nlp_classify(title: str, hint: Optional[str] = None) -> tuple[str, float]:
    """Returns (group, confidence_gap)."""
    model, group_embeddings = _load_nlp_model()
    if model is None:
        return hint or "Others", 0.0

    import numpy as np
    query = f"{hint}: {title}" if hint else title
    emb   = model.encode(query, normalize_embeddings=True)
    scores = {name: float(np.dot(emb, vec)) for name, vec in group_embeddings.items()}

    if hint and hint in scores:
        scores[hint] *= 1.10

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    winner = sorted_scores[0][0]
    gap    = sorted_scores[0][1] - sorted_scores[1][1] if len(sorted_scores) > 1 else 1.0
    return winner, round(gap, 4)


# =============================================================================
# FULL PIPELINE
# =============================================================================

NLP_GAP_THRESHOLD = 0.035


def _classify(title: str) -> dict:
    """
    Full two-layer classification. Returns dict with final_group and metadata.
    Internal use — public API is extract_procurement_group().
    """
    l1 = _layer1_classify(title)

    if l1["decision"] == "STOP":
        final, nlp_group, nlp_gap = l1["winner"], "—", 1.0
    elif l1["decision"] == "HINT":
        nlp_group, nlp_gap = _nlp_classify(title, hint=l1["winner"])
        final = nlp_group
    else:
        nlp_group, nlp_gap = _nlp_classify(title, hint=None)
        final = nlp_group

    return {
        "final_group":       final,
        "layer1_decision":   l1["decision"],
        "layer1_group":      l1["winner"],
        "layer1_confidence": l1["confidence"],
        "layer1_keywords":   l1["matched_keywords"],
        "nlp_group":         nlp_group,
        "nlp_gap":           nlp_gap,
    }


# =============================================================================
# PUBLIC API
# =============================================================================

def extract_procurement_group(title: str) -> str:
    """
    Extract procurement group from a tender title.
    Always returns a string — never None or null.
    Returns one of: CONSULTING | WORKS | GOODS | NON-CONSULTING | Others
    """
    if not title or not title.strip():
        return "Others"
    return _classify(title)["final_group"]


def get_procurement_group(
    title:    str,
    existing: Optional[str] = None,
) -> str:
    """
    Get procurement group with merge rules.
    If existing is already populated (non-null, non-empty), keeps it.
    Always returns a string — never None or null.
    """
    if existing and existing.strip():
        return existing
    return extract_procurement_group(title)


# =============================================================================
# SELF-TEST
# =============================================================================

KNOWN_ERRORS = [
    # (title, expected_group, error_type)
    ("Consultancy - Review of Drug-Resistant Tuberculosis (DR-TB)", "CONSULTING", "T1"),
    ("End-of-Term Review and Development of the National Strategic Plan (NSP)", "CONSULTING", "T1"),
    ("IC-Market & Demand Assessment for Micro-Takaful and Bundled Loan Products", "CONSULTING", "T1"),
    ("Pilot Study on Raast P2M Usage", "CONSULTING", "T1"),
    ("Procurement of National Project Consultant for TIAEWS and RIDS Projects", "CONSULTING", "T1"),
    ("RfP for the delivery of 01 training program on digital literacy for youth", "CONSULTING", "T1"),
    ("Consultant(e) national(e) pour le suivi-contrôle des travaux de Rehabilitation", "CONSULTING", "T1"),
    ("019-Structural Engineer", "CONSULTING", "T1"),
    ("IC - Development of Climate-Responsive Insurance Product for Seaweed Farmers", "CONSULTING", "T1"),
    ("Sourcing in the Recycling Value Chain", "CONSULTING", "T1"),
    ("Producción de seis Paquetes Tecnológicos", "CONSULTING", "T1"),
    ("Elaboración de Hoja de Ruta para el Sector Cementero de Costa Rica", "CONSULTING", "T1"),
    ("Servicio de extracción de ARN total y Secuenciación RNA-Seq", "CONSULTING", "T1"),
    ("Furniture for the SESU modular training centre in Cherkasy", "GOODS", "T2"),
    ("Conservation works at 2 Sites; Panagia Chryseleousa and Panagia Mnasi", "WORKS", "T2"),
    ("GPSDH GPH338901 Invitation for Prequalification LENACAPAVIR", "GOODS", "T2"),
    ("RFQ_Improvement of functionality of the indoor sport facilities", "WORKS", "T2"),
    ("Impression des outils de collecte des données du PNLS", "NON-CONSULTING", "T2"),
    ("Equipos y Herramientas del Almacén del ITSE", "GOODS", "T2"),
    ("CONSULTATION POUR LA FOURNITURE DES EQUIPEMENTS DE CLIMATISATION", "GOODS", "R3"),
    ("Fourniture de carburant aux agences des Nations Unies en RDC", "GOODS", "R3"),
    ("Team Building Support to SPPU/TMS Team Retreat", "NON-CONSULTING", "R3"),
    ("Production and installation of integrated AC/DC power supply cabinets", "WORKS", "R3"),
    ("Adquisición Cámaras de Seguridad y Controles de Acceso", "GOODS", "R3"),
    ("Procurement of Software and Equipment for waste management", "GOODS", "R3"),
    ("Global LTA for Supply of Agricultural Tractors and Construction Machineries", "GOODS", "R3"),
    ("Long Term Office Rent/Solutions for AI Hub in Rome Italy", "NON-CONSULTING", "R3"),
    ("Provision of Cleaning & Gardening Services on LTA Basis at the UN House in Dili", "NON-CONSULTING", "R3"),
    ("Transportation Services for Distribution of Health Products", "NON-CONSULTING", "R3"),
]


def run_self_test():
    print("\n" + "=" * 70)
    print(f"SELF-TEST — {len(KNOWN_ERRORS)} known error cases (Layer 1 only)")
    print("=" * 70)

    fixed = still_wrong = needs_nlp = 0

    for title, expected, etype in KNOWN_ERRORS:
        r         = _layer1_classify(title)
        predicted = r["winner"]
        decision  = r["decision"]
        conf      = r["confidence"]

        if predicted == expected:
            if decision == "STOP":
                status = "✅ FIXED (STOP)"
                fixed += 1
            else:
                status = "🟡 FIXED (→NLP)"
                needs_nlp += 1
                fixed += 1
        else:
            status = "❌ STILL WRONG"
            still_wrong += 1

        print(f"\n{status}  [{etype}]  expected={expected}  got={predicted}  conf={conf}%")
        print(f"  {title[:80]}")
        if r["matched_keywords"]:
            print(f"  kw: {r['matched_keywords'][:100]}")

    print("\n" + "-" * 70)
    print(f"Fixed : {fixed}/{len(KNOWN_ERRORS)}  "
          f"(STOP={fixed - needs_nlp}, →NLP={needs_nlp}, wrong={still_wrong})")
    print()


# =============================================================================
# DATABASE UPDATE RUNNER
# =============================================================================

def run_update(
    mode:    str  = "nulls-and-others",
    dry_run: bool = False,
    limit:   Optional[int] = None,
) -> None:
    """
    Update procurement_group on enriched_tenders rows.
    Runs on ALL portals — wherever procurement_group is null or Others.
    Does not touch enrichment_status.
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
    else:  # nulls-and-others (default)
        where = "WHERE e.procurement_group IS NULL OR e.procurement_group = 'Others'"

    limit_clause = f"LIMIT {limit}" if limit else ""

    select_sql = f"""
        SELECT e.id, t.title, e.procurement_group, e.source_portal
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
        log.info("Found %d rows to process (mode: %s, all portals)", total, mode)

        if total == 0:
            log.info("Nothing to do.")
            return

        batch_params = []

        for row in rows:
            new_group = extract_procurement_group(row["title"] or "")

            if dry_run:
                log.info(
                    "  [DRY-RUN] id=%-6s  portal=%-8s  old=%-20s  new=%-20s  title=%s",
                    row["id"],
                    row["source_portal"] or "",
                    row["procurement_group"] or "NULL",
                    new_group,
                    (row["title"] or "")[:70],
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


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract and update procurement_group on enriched_tenders (all portals)."
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
    parser.add_argument("--self-test", action="store_true",
        help="Run Layer 1 self-test against known error cases.")

    args = parser.parse_args()

    if args.self_test:
        run_self_test()
    else:
        run_update(mode=args.mode, dry_run=args.dry_run, limit=args.limit)