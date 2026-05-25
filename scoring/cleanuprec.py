"""
cleanuprec.py
================
KPMG Tender Pipeline — Daily Cleanup
Runs as Step 0 in the orchestrator, before scrapers.

What it does:
1. Recomputes days_to_deadline for ALL enriched tenders
   (deadline_datetime is stored, days_to_deadline goes stale daily)
2. Hides expired tenders from pipeline by setting p_go = NULL
   (does NOT delete tender_scores — scores are preserved for detail view)
3. Resets p_go to NULL for tenders with < 2 days remaining
4. Prints a summary

Run standalone:
    cd C:\\projects\\tenders
    python scoring\\cleanuprec.py
"""

import sys
import os
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db import get_session
from models import EnrichedTender, TenderScore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

NOW = datetime.now(timezone.utc)


def run_daily_cleanup():
    logger.info("=" * 55)
    logger.info("  KPMG DAILY CLEANUP — Starting")
    logger.info(f"  Run date: {NOW.strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("=" * 55)

    stats = {
        "deadlines_refreshed":  0,
        "expired_hidden":       0,
        "expired_p_go_reset":   0,
        "errors":               0,
    }

    with get_session() as session:

        # ── Step 1: Refresh days_to_deadline for all tenders ─────────────
        logger.info("  Step 1 — Refreshing days_to_deadline...")

        tenders = (
            session.query(EnrichedTender)
            .filter(EnrichedTender.deadline_datetime.isnot(None))
            .all()
        )

        for tender in tenders:
            try:
                dl = tender.deadline_datetime
                if dl.tzinfo is None:
                    dl = dl.replace(tzinfo=timezone.utc)
                tender.days_to_deadline = (dl - NOW).days
                stats["deadlines_refreshed"] += 1
            except Exception as e:
                logger.error(f"  Error refreshing tender {tender.id}: {e}")
                stats["errors"] += 1

        session.commit()
        logger.info(f"  ✅ Refreshed {stats['deadlines_refreshed']} deadlines")

        # ── Step 2: Hide expired tenders from pipeline ────────────────────
        # Sets p_go = NULL so they don't appear in dashboard/alerts
        # Does NOT delete tender_scores — scores preserved for detail view
        logger.info("  Step 2 — Hiding expired tenders from pipeline...")

        try:
            expired_ids = [
                t.id for t in tenders
                if t.days_to_deadline is not None and t.days_to_deadline < 0
            ]

            if expired_ids:
                expired_tenders = (
                    session.query(EnrichedTender)
                    .filter(EnrichedTender.id.in_(expired_ids))
                    .all()
                )
                for t in expired_tenders:
                    t.p_go = None  # hide from pipeline — score preserved in tender_scores

                session.commit()
                stats["expired_hidden"] = len(expired_ids)
                logger.info(f"  ✅ Hidden {len(expired_ids)} expired tenders from pipeline")
            else:
                logger.info("  ✅ No expired tenders to hide")

        except Exception as e:
            logger.error(f"  Error hiding expired tenders: {e}")
            stats["errors"] += 1

        # ── Step 3: Reset p_go for tenders with < 2 days remaining ────────
        # So they won't be shown in alerts or re-scored
        logger.info("  Step 3 — Resetting p_go for imminent tenders (< 2 days)...")

        try:
            imminent = (
                session.query(EnrichedTender)
                .filter(
                    EnrichedTender.days_to_deadline < 2,
                    EnrichedTender.days_to_deadline >= 0,
                    EnrichedTender.p_go.isnot(None),
                )
                .all()
            )

            for tender in imminent:
                tender.p_go            = None
                tender.score_breakdown = None
                tender.model_version   = None
                stats["expired_p_go_reset"] += 1

            session.commit()
            logger.info(f"  ✅ Reset p_go for {stats['expired_p_go_reset']} imminent tenders")

        except Exception as e:
            logger.error(f"  Error resetting imminent tenders: {e}")
            stats["errors"] += 1

    # ── Summary ───────────────────────────────────────────────────────────
    logger.info("=" * 55)
    logger.info("  CLEANUP COMPLETE")
    logger.info(f"  Deadlines refreshed : {stats['deadlines_refreshed']}")
    logger.info(f"  Expired hidden      : {stats['expired_hidden']}")
    logger.info(f"  Imminent reset      : {stats['expired_p_go_reset']}")
    logger.info(f"  Errors              : {stats['errors']}")
    logger.info("=" * 55)


if __name__ == "__main__":
    run_daily_cleanup()