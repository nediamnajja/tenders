"""
orchestrator.py
===============
KPMG Tender Pipeline — Main Orchestrator
Triggered every day at 7:00 AM by Windows Task Scheduler.

Pipeline order:
  Step 1  — Scrapers (4 portals, sequential)
  Step 2  — Normalizer
  Step 3  — Enrichment: normalisationseed0
  Step 4  — Enrichment: pdf_extractor1
  Step 5  — Enrichment: stage2
  Step 6  — Enrichment: stage3 (NLP sectors)
  Step 7  — Enrichment: procurement classifier
  Step 8  — Scoring: logistic regression
  Step 9  — Email alert
  [ Step 10 — LLM enrichment — placeholder, not built yet ]

Logs:
  Every run writes to C:\\projects\\tenders\\logs\\pipeline_YYYY-MM-DD.log
  Errors are clearly marked so you can diagnose failures fast.

Run manually:
    cd C:\\projects\\tenders
    python orchestrator.py
"""

import subprocess
import sys
import os
import logging
from datetime import datetime, date

# ─────────────────────────────────────────────────────────────
#  PATHS
# ─────────────────────────────────────────────────────────────

ROOT      = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR  = os.path.join(ROOT, "logs")
TODAY     = date.today().isoformat()
LOG_FILE  = os.path.join(LOGS_DIR, f"pipeline_{TODAY}.log")
PYTHON    = sys.executable   # uses the same Python that runs this script

os.makedirs(LOGS_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
#  LOGGING — writes to both terminal and log file
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s  %(levelname)s  %(message)s",
    datefmt  = "%H:%M:%S",
    handlers = [
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  PIPELINE STEPS
#  Each entry: (label, script_path)
#  Paths are relative to ROOT (C:\projects\tenders\)
# ─────────────────────────────────────────────────────────────

PIPELINE = [
    # ── STEP 1: Scrapers ────────────────────────────────────
    ("Scraper — AFDP",       "scraper_afdp_daily.py"),
    ("Scraper — UNDP",       "scraper_undp_daily.py"),
    ("Scraper — UNGM",       "scraper_ungm_daily.py"),
    ("Scraper — World Bank", "scraper_worldbank_daily.py"),

    # ── STEP 2: Normalizer ───────────────────────────────────
    ("Normalizer",           os.path.join("normalizer", "common.py")),

    # ── STEP 3-7: Enrichment ─────────────────────────────────
    ("Enrichment — Seed",          os.path.join("enricher", "normalisationseed0.py")),
    ("Enrichment — PDF Extractor", os.path.join("enricher", "pdf_extractor1.py")),
    ("Enrichment — Stage 2",       os.path.join("enricher", "stage2.py")),
    ("Enrichment — Stage 3 NLP",   os.path.join("enricher", "stage3.py")),
    ("Enrichment — Procurement",   os.path.join("enricher", "procurment.py")),

    # ── STEP 8: Scoring ──────────────────────────────────────
    ("Scoring — Logistic Regression", os.path.join("scoring", "logistic_regression.py")),

    # ── STEP 9: Email Alert ──────────────────────────────────
    ("Email Alert", "alerts/email_alert.py"),

    # ── STEP 10: LLM Enrichment — NOT BUILT YET ─────────────
    # ("LLM Enrichment", os.path.join("enricher", "llm_enrichment.py")),
]

# ─────────────────────────────────────────────────────────────
#  RUN A SINGLE STEP
# ─────────────────────────────────────────────────────────────

def run_step(label: str, script_path: str) -> bool:
    """
    Run a single pipeline step as a subprocess.
    Returns True if successful, False if it failed.
    Working directory is always ROOT so all relative imports work.
    """
    full_path = os.path.join(ROOT, script_path)

    if not os.path.exists(full_path):
        logger.error(f"  ❌  MISSING FILE: {full_path}")
        return False

    logger.info(f"  ▶  {label}")
    start = datetime.now()

    try:
        result = subprocess.run(
            [PYTHON, full_path],
            cwd            = ROOT,          # always run from tenders\ root
            capture_output = True,
            text           = True,
            timeout        = 3600,          # 1 hour max per step
        )

        duration = (datetime.now() - start).seconds

        # Write step output to log file
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                logger.info(f"     {line}")
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                logger.warning(f"     {line}")

        if result.returncode == 0:
            logger.info(f"  ✅  {label} — done in {duration}s")
            return True
        else:
            logger.error(f"  ❌  {label} — FAILED (exit code {result.returncode}) in {duration}s")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"  ❌  {label} — TIMEOUT after 3600s")
        return False
    except Exception as e:
        logger.error(f"  ❌  {label} — ERROR: {e}")
        return False

# ─────────────────────────────────────────────────────────────
#  MAIN PIPELINE RUNNER
# ─────────────────────────────────────────────────────────────

def run_pipeline():
    start_time = datetime.now()

    logger.info("=" * 65)
    logger.info(f"  KPMG TENDER PIPELINE — {TODAY}  {start_time.strftime('%H:%M:%S')}")
    logger.info("=" * 65)

    stats = {
        "total":   len(PIPELINE),
        "success": 0,
        "failed":  0,
        "skipped": 0,
    }

    failed_steps = []

    for i, (label, script) in enumerate(PIPELINE, 1):
        logger.info(f"\n{'─'*65}")
        logger.info(f"  STEP {i}/{stats['total']} — {label}")
        logger.info(f"{'─'*65}")

        success = run_step(label, script)

        if success:
            stats["success"] += 1
        else:
            stats["failed"] += 1
            failed_steps.append(label)

            # ── CRITICAL STEPS: stop pipeline if these fail ──
            # If scrapers all fail or scoring fails, no point continuing
            critical = ["Scoring — Logistic Regression"]
            if label in critical:
                logger.error(f"\n  🛑  Critical step failed — stopping pipeline")
                stats["skipped"] = stats["total"] - i
                break

            # ── NON-CRITICAL: log and continue ───────────────
            logger.warning(f"  ⚠️   Non-critical step failed — continuing pipeline")

    # ── SUMMARY ─────────────────────────────────────────────
    duration = (datetime.now() - start_time).seconds
    minutes  = duration // 60
    seconds  = duration % 60

    logger.info(f"\n{'='*65}")
    logger.info(f"  PIPELINE COMPLETE — {minutes}m {seconds}s")
    logger.info(f"{'='*65}")
    logger.info(f"  Total steps : {stats['total']}")
    logger.info(f"  Succeeded   : {stats['success']}")
    logger.info(f"  Failed      : {stats['failed']}")
    logger.info(f"  Skipped     : {stats['skipped']}")

    if failed_steps:
        logger.error(f"\n  Failed steps:")
        for s in failed_steps:
            logger.error(f"    ❌  {s}")
    else:
        logger.info(f"\n  All steps completed successfully ✅")

    logger.info(f"\n  Log saved to: {LOG_FILE}")
    logger.info(f"{'='*65}\n")

    return stats["failed"] == 0


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)