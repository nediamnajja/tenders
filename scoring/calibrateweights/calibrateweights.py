"""
calibrate_weights.py
====================
Finds the optimal scale + baseline so that:
  - ~5 tenders score >= 85% (STRONG GO)
  - Reasonable number score >= 75% (GO)
  - Distribution looks natural

Run:
    cd C:\projects\tenders
    python scoring\calibrate_weights.py
"""

import math
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import get_session
from models import EnrichedTender, WeightsHistory
from logistic_regression import build_feature_vector, FEATURE_TO_COLUMN

# =============================================================================
# TARGETS
# =============================================================================
TARGET_STRONG_GO_MAX = 8    # we want at most ~5-8 tenders above 85%
TARGET_GO_MAX        = 40   # reasonable GO count
THRESHOLD_STRONG_GO  = 0.75
THRESHOLD_GO         = 0.65

# =============================================================================
# LOAD WEIGHTS
# =============================================================================

def load_weights(session, version=2):
    """
    Load weights from a specific version (default=2, the original trained weights).
    Always calibrate from the original — never from an already-scaled version.
    """
    row = (
        session.query(WeightsHistory)
        .filter(WeightsHistory.version == version)
        .first()
    )
    if not row:
        # Fallback to earliest version
        row = (
            session.query(WeightsHistory)
            .order_by(WeightsHistory.version.asc())
            .first()
        )
    col_to_feature = {v: k for k, v in FEATURE_TO_COLUMN.items()}
    weights = {}
    for col_name, feature_name in col_to_feature.items():
        val = getattr(row, col_name, None)
        if val is not None:
            weights[feature_name] = float(val)
    print(f"  Using original weights version {row.version} as calibration base")
    return weights, float(row.baseline), int(row.version)


def sigmoid(z):
    return 1 / (1 + math.exp(-z))


def score_tender(features, weights, baseline, scale):
    """Score with scaled positive weights."""
    Z = baseline
    for feature, x in features.items():
        if x == 1 and feature in weights:
            w = weights[feature]
            Z += (w * scale) if w > 0 else w
    return sigmoid(Z)


# =============================================================================
# MAIN CALIBRATION
# =============================================================================

def run_calibration():
    print("\n" + "="*65)
    print("  KPMG WEIGHT CALIBRATOR")
    print("  Target: ~5 STRONG GO (>=85%), reasonable GO (>=75%)")
    print("="*65)

    with get_session() as session:
        weights, original_baseline, version = load_weights(session)
        print(f"\n  Loaded weights version {version}")

        # Fetch all scored tenders with valid deadlines
        tenders = (
            session.query(EnrichedTender)
            .filter(
                EnrichedTender.enrichment_status.in_(["rules_complete", "nlp_complete"]),
                EnrichedTender.days_to_deadline >= 2,
            )
            .all()
        )

        # Build feature vectors inside session
        tender_features = []
        for t in tenders:
            try:
                features = build_feature_vector(t)
                tender_features.append(features)
            except Exception:
                continue

    print(f"  Tenders to score: {len(tender_features)}")
    print(f"\n  Testing scale x baseline combinations...")
    print(f"\n  {'Scale':>6}  {'Baseline':>9}  {'>=85%':>7}  {'>=75%':>7}  {'>=65%':>7}  {'<65%':>7}  {'Verdict'}")
    print(f"  {'-'*6}  {'-'*9}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*20}")

    best_combo = None
    best_score_diff = 999

    # Test combinations
    scales    = [0.30, 0.33, 0.36, 0.38, 0.40, 0.42, 0.44, 0.46, 0.48, 0.50, 0.55, 0.60]
    baselines = [-2.0, -2.5, -3.0, -3.5, -4.0, -4.5, -4.689, -5.0, -5.5, -6.0]

    results = []
    for scale in scales:
        for baseline in baselines:
            scores = [
                score_tender(f, weights, baseline, scale)
                for f in tender_features
            ]
            n_strong = sum(1 for s in scores if s >= THRESHOLD_STRONG_GO)
            n_go     = sum(1 for s in scores if THRESHOLD_GO <= s < THRESHOLD_STRONG_GO)
            n_mid    = sum(1 for s in scores if 0.65 <= s < THRESHOLD_GO)
            n_below  = sum(1 for s in scores if s < 0.65)

            # Score quality: strong_go close to 5, go reasonable
            diff = abs(n_strong - 5) + max(0, n_go - 40) * 0.5
            results.append((diff, scale, baseline, n_strong, n_go, n_mid, n_below))

    # Sort by best fit
    results.sort(key=lambda x: x[0])

    # Print top 15
    for i, (diff, scale, baseline, n_strong, n_go, n_mid, n_below) in enumerate(results[:15]):
        verdict = ""
        if n_strong <= 8 and n_strong >= 2 and n_go <= 50:
            verdict = "✅ GOOD"
        elif n_strong <= 12:
            verdict = "⚠️  acceptable"
        else:
            verdict = "❌ too many"
        print(f"  {scale:>6.2f}  {baseline:>9.2f}  {n_strong:>7}  {n_go:>7}  {n_mid:>7}  {n_below:>7}  {verdict}")

    # Best combo
    best = results[0]
    _, best_scale, best_baseline, n_strong, n_go, n_mid, n_below = best

    print(f"\n{'='*65}")
    print(f"  BEST COMBINATION:")
    print(f"  Scale    = {best_scale}")
    print(f"  Baseline = {best_baseline}")
    print(f"  STRONG GO (>=85%) : {n_strong}")
    print(f"  GO (75-84%)       : {n_go}")
    print(f"  Near miss (65-74%): {n_mid}")
    print(f"  Below (< 65%)     : {n_below}")
    print(f"{'='*65}")

    # Show what actual new weights would be
    print(f"\n  SQL to insert new weights:")
    print(f"  UPDATE weights_history SET baseline = {best_baseline}")
    print(f"  (and multiply all positive weights by {best_scale})")
    print(f"\n  Run this script, pick a combination, then run:")
    print(f"  python scoring\\apply_calibration.py --scale {best_scale} --baseline {best_baseline}")


if __name__ == "__main__":
    run_calibration()