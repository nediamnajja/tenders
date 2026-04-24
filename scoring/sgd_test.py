"""
interactive_sgd.py
==================
KPMG SGD — Interactive Partner Decision Terminal

Flow per tender:
1. Display tender card (title, country, sector, budget, agency, deadline, p_go)
2. Partner types G / N / S / Q
3. YES/NO questions per active feature  -> j values set directly
4. SGD runs:  alpha = alpha + eta x (y - p) x j x x
5. Weights saved to DB as new version after every decision

Run:
    cd C:\projects\tenders
    python scoring\interactive_sgd.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from db import get_session
from models import EnrichedTender
from sgd_updater import (
    load_latest_weights,
    save_updated_weights,
    ask_relevance_questions,
    sgd_update,
    print_update_report,
    FEATURE_LABELS,
    label,
)
from logistic_regression import build_feature_vector
from copy import deepcopy

# =============================================================================
# FETCH SCORED TENDERS
# =============================================================================

def fetch_scored_tenders(session):
    return (
        session.query(EnrichedTender)
        .filter(EnrichedTender.p_go.isnot(None))
        .order_by(EnrichedTender.p_go.desc())
        .all()
    )

# =============================================================================
# DISPLAY TENDER CARD
# =============================================================================

def display_tender(tender, index: int, total: int, weights: dict, baseline: float):
    import ast

    # Recompute contributions for display
    features = build_feature_vector(tender)
    contributions = {
        f: round(weights.get(f, 0.0) * x, 4)
        for f, x in features.items()
        if x == 1 and weights.get(f, 0.0) != 0
    }
    top3 = sorted(contributions.items(), key=lambda kv: -abs(kv[1]))[:3]

    # Clean up sector display
    sector_raw = tender.sector or "Unknown"
    try:
        parsed = ast.literal_eval(sector_raw)
        if isinstance(parsed, list):
            sector_str = ", ".join(parsed)
        else:
            sector_str = sector_raw
    except:
        sector_str = sector_raw
    if len(sector_str) > 55:
        sector_str = sector_str[:52] + "..."

    budget_str   = f"{tender.budget:,.0f} EUR" if tender.budget else "Unknown"
    deadline_str = f"{int(tender.days_to_deadline)} days" if tender.days_to_deadline else "Unknown"

    # p_go color indicator
    p = tender.p_go
    if p >= 0.80:   verdict = "STRONG GO ✅✅"
    elif p >= 0.70: verdict = "GO ✅"
    elif p >= 0.50: verdict = "UNCERTAIN ⚠️"
    else:           verdict = "NO GO ❌"

    print(f"\n{'━'*64}")
    print(f"  TENDER {index}/{total}   P(GO) = {p:.1%}   {verdict}")
    print(f"{'━'*64}")
    print(f"  {'Title':<16}: {(tender.title_clean or 'N/A')[:60]}")
    print(f"  {'Country':<16}: {tender.country_name_normalized or 'Unknown'}")
    print(f"  {'Sector':<16}: {sector_str}")
    print(f"  {'Procurement':<16}: {tender.procurement_group or 'Unknown'}")
    print(f"  {'Agency':<16}: {tender.funding_agency or 'Unknown'}")
    print(f"  {'Budget':<16}: {budget_str}")
    print(f"  {'Deadline':<16}: {deadline_str}")
    print(f"{'─'*64}")
    print(f"  Top signals that drove this score:")
    for feat, val in top3:
        lbl  = FEATURE_LABELS.get(feat, feat)
        bar  = "█" * min(int(abs(val) * 8), 30)
        sign = "+" if val >= 0 else "-"
        print(f"    {sign} {lbl:<42} {val:+.3f}  {bar}")
    print(f"{'━'*64}")

# =============================================================================
# GET DECISION
# =============================================================================

def get_decision() -> tuple:
    """Returns (y, status) where status = OK / SKIP / QUIT"""
    print("\n  Your decision:")
    print("    [G]  GO")
    print("    [N]  NO GO")
    print("    [S]  Skip")
    print("    [Q]  Quit & save")

    while True:
        ans = input("\n  → ").strip().upper()
        if ans in ("Q", "QUIT"):   return None, "QUIT"
        if ans in ("S", "SKIP"):   return None, "SKIP"
        if ans in ("G", "GO", "1"):
            print("  Decision: ✅ GO")
            return 1, "OK"
        if ans in ("N", "NO", "NOGO", "NO GO", "0"):
            print("  Decision: ❌ NO GO")
            return 0, "OK"
        print("  ⚠️  Please type G, N, S, or Q")

# =============================================================================
# SESSION SUMMARY
# =============================================================================

def print_session_summary(log: list, w0: dict, w1: dict, b0: float, b1: float):
    go_count   = sum(1 for d in log if d["y"] == 1)
    nogo_count = sum(1 for d in log if d["y"] == 0)

    print(f"\n\n{'█'*64}")
    print(f"  SESSION SUMMARY")
    print(f"{'█'*64}")
    print(f"  Tenders reviewed : {len(log)}")
    print(f"  GO               : {go_count}")
    print(f"  NO GO            : {nogo_count}")

    print(f"\n  TOP WEIGHT CHANGES THIS SESSION:")
    print(f"  {'Feature':<44} {'Before':>9}  {'After':>9}  {'Δ':>9}")
    print(f"  {'-'*44} {'-'*9}  {'-'*9}  {'-'*9}")

    changes = [(f, w0[f], w1.get(f, w0[f]), w1.get(f, w0[f]) - w0[f]) for f in w0]
    for feat, old, new, delta in sorted(changes, key=lambda x: -abs(x[3]))[:15]:
        marker = " ▲" if delta > 0.001 else (" ▼" if delta < -0.001 else "  ")
        print(f"  {label(feat):<44} {old:>9.5f}  {new:>9.5f}  {delta:>+9.5f}{marker}")

    print(f"\n  {'Baseline':<44} {b0:>9.5f}  {b1:>9.5f}  {b1-b0:>+9.5f}")
    print(f"\n{'█'*64}\n")

# =============================================================================
# MAIN LOOP
# =============================================================================

def run_interactive_session():
    print("\n" + "█"*64)
    print("  KPMG SGD — Interactive Partner Decision Terminal")
    print("█"*64)
    print("\n  Connecting to database...")

    with get_session() as session:

        try:
            weights, baseline, version = load_latest_weights(session)
            print(f"  ✅ Weights version {version} loaded")
        except RuntimeError as e:
            print(f"  ❌ {e}")
            return

        tenders = fetch_scored_tenders(session)
        if not tenders:
            print("  ❌ No scored tenders found. Run scoring_engine.py first.")
            return

        print(f"  ✅ {len(tenders)} scored tenders found (sorted by P(GO) desc)")
        print(f"\n  HOW IT WORKS:")
        print(f"  1. Review the tender card")
        print(f"  2. Type G=GO or N=NO GO")
        print(f"  3. Answer YES/NO for each factor that was on your mind")
        print(f"     Y → j=1.0  full weight update")
        print(f"     N → j=0.1  small nudge (model still learns a little)")
        print(f"  4. Weights saved to DB after every decision")
        print(f"  5. Type S to skip, Q to quit anytime")

        input(f"\n  Press ENTER to start...\n")

        initial_weights  = deepcopy(weights)
        initial_baseline = baseline
        decisions_log    = []
        current_version  = version

        for i, tender in enumerate(tenders, 1):

            display_tender(tender, i, len(tenders), weights, baseline)

            y, status = get_decision()

            if status == "QUIT":
                print("\n  Quitting...")
                break
            if status == "SKIP":
                print(f"  ↩  Skipped")
                continue

            # Build feature vector
            features        = build_feature_vector(tender)
            active_features = {k: v for k, v in features.items() if v == 1}

            # YES/NO questions → j values
            j_values, other_reason = ask_relevance_questions(active_features)

            # SGD update
            weights, baseline, update_log = sgd_update(
                weights  = weights,
                baseline = baseline,
                features = features,
                y        = y,
                p        = float(tender.p_go),
                j_values = j_values,
            )

            # Print what changed
            print_update_report(
                f"Tender {i}/{len(tenders)} — {'GO' if y==1 else 'NO GO'}",
                update_log,
                features,
            )

            # Log
            decisions_log.append({
                "tender_id":    tender.id,
                "title":        tender.title_clean,
                "y":            y,
                "p":            float(tender.p_go),
                "error":        update_log["error"],
                "other_reason": other_reason,
            })

            # Save to DB after every decision
            notes = (
                f"{'GO' if y==1 else 'NO GO'} on tender {tender.id} "
                f"(p={tender.p_go:.2f})"
                + (f" | other: {other_reason[:60]}" if other_reason else "")
            )
            current_version = save_updated_weights(
                session     = session,
                weights     = weights,
                baseline    = baseline,
                old_version = current_version,
                notes       = notes,
            )
            print(f"\n  💾 Weights saved → version {current_version}")

            if i < len(tenders):
                cont = input("\n  Next tender? [ENTER=yes / Q=quit] → ").strip().upper()
                if cont in ("Q", "QUIT"):
                    break

        if decisions_log:
            print_session_summary(
                log = decisions_log,
                w0  = initial_weights,
                w1  = weights,
                b0  = initial_baseline,
                b1  = baseline,
            )

        print(f"  Final weights version : {current_version}")
        print(f"  Session complete ✅\n")


if __name__ == "__main__":
    run_interactive_session()