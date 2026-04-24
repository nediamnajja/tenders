"""
tenders_report.py
==================
Reads ALL scored tenders from PostgreSQL (GO and NO GO)
and exports them to a CSV file.

Run:
    python scoring/tenders_report.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import csv
import ast
from datetime import datetime

from db import get_session
from models import EnrichedTender


# =============================================================================
# HELPERS
# =============================================================================

def parse_sectors(s: str) -> str:
    """Parse sector JSON array into readable string."""
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return ", ".join(parsed)
    except:
        pass
    return s or "Unknown"


def format_budget(budget, currency) -> str:
    if budget:
        return f"{budget:,.0f} {currency or ''}".strip()
    return "Unknown"


def get_recommendation(p_go: float) -> str:
    if p_go is None:
        return "NOT SCORED"
    if p_go >= 0.80:
        return "STRONG GO 🟢"
    if p_go >= 0.70:
        return "GO 🟡"
    return "NO GO 🔴"


# =============================================================================
# MAIN
# =============================================================================

def export_all_tenders(limit: int = 5000):
    """
    Read all scored tenders from enriched_tenders
    and export to CSV — both GO and NO GO.
    """

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M")
    output_file = f"tenders_report_{timestamp}.csv"

    with get_session() as session:

        # Fetch all scored tenders ordered by p_go descending
        tenders = (
            session.query(EnrichedTender)
            .filter(EnrichedTender.p_go.isnot(None))
            .filter(EnrichedTender.days_to_deadline >= 2)
            .order_by(EnrichedTender.p_go.desc())
            .limit(limit)
            .all()
        )

        if not tenders:
            print("No scored tenders found.")
            print("Make sure you ran scoring_engine.py first.")
            return

        # ── Terminal summary ──────────────────────────────────
        print()
        print("=" * 70)
        print(f"  TENDERS REPORT — {len(tenders)} scored tenders")
        print("=" * 70)

        strong_go = sum(1 for t in tenders if t.p_go and t.p_go >= 0.80)
        go        = sum(1 for t in tenders if t.p_go and 0.70 <= t.p_go < 0.80)
        no_go     = sum(1 for t in tenders if t.p_go and t.p_go < 0.70)

        print(f"  STRONG GO  (≥80%) : {strong_go}")
        print(f"  GO         (70-79%): {go}")
        print(f"  NO GO      (<70%)  : {no_go}")
        print(f"  Total              : {len(tenders)}")
        print("=" * 70)

        # ── Write CSV ─────────────────────────────────────────
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Header
            writer.writerow([
                "Rank",
                "Recommendation",
                "P(GO) %",
                "Title",
                "Country",
                "Sector",
                "Procurement",
                "Funding Agency",
                "Budget",
                "Days to Deadline",
                "Model Version",
                "Scored At",
                "Source Portal",
                "Source URL",
            ])

            # Rows
            for rank, tender in enumerate(tenders, 1):
                writer.writerow([
                    rank,
                    get_recommendation(tender.p_go),
                    f"{tender.p_go:.1%}" if tender.p_go else "N/A",
                    tender.title_clean or "N/A",
                    tender.country_name_normalized or "N/A",
                    parse_sectors(tender.sector) if tender.sector else "N/A",
                    tender.procurement_group or "N/A",
                    tender.funding_agency or "N/A",
                    format_budget(tender.budget, tender.currency),
                    tender.days_to_deadline or "N/A",
                    tender.model_version or "N/A",
                    tender.enriched_at.strftime("%Y-%m-%d") if tender.enriched_at else "N/A",
                    tender.source_portal or "N/A",
                    tender.source_url or "N/A",
                ])

        print(f"\n  ✅ CSV exported → {output_file}")
        print(f"  Open it in Excel to explore the results.")
        print("=" * 70)

        # ── Preview top 10 in terminal ────────────────────────
        print("\n  TOP 10 PREVIEW:")
        print(f"  {'─' * 68}")
        print(f"  {'#':<4} {'REC':<12} {'P(GO)':<8} {'COUNTRY':<20} {'TITLE':<30}")
        print(f"  {'─' * 68}")

        for rank, tender in enumerate(tenders[:10], 1):
            rec     = get_recommendation(tender.p_go)
            p_go    = f"{tender.p_go:.1%}" if tender.p_go else "N/A"
            country = (tender.country_name_normalized or "N/A")[:18]
            title   = (tender.title_clean or "N/A")[:28]
            # strip emoji for clean terminal alignment
            rec_clean = rec.replace("🟢","").replace("🟡","").replace("🔴","").strip()
            print(f"  {rank:<4} {rec_clean:<12} {p_go:<8} {country:<20} {title}")

        print(f"  {'─' * 68}")
        print(f"\n  Full results in: {output_file}")


if __name__ == "__main__":
    export_all_tenders()