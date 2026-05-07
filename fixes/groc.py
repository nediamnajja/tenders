"""
procurement_groq_validator.py
==============================
Layer 3 — Groq LLM validator for CONSULTING classification.
Runs AFTER procurement_group_v2.py has produced its output CSV.

Two specific jobs:
  Job 1 — VERIFY:  title predicted as CONSULTING  → confirm it really is
  Job 2 — RESCUE:  title predicted as NON-CONSULTING → check if it's actually CONSULTING

Only processes rows where needs_groq == YES, or optionally ALL CONSULTING
and NON-CONSULTING rows for a full audit pass.

Usage:
    # Validate only the flagged uncertain rows (recommended for production)
    python procurement_groq_validator.py --input procurement_pipeline_v2_results.csv

    # Full audit — all CONSULTING + NON-CONSULTING rows
    python procurement_groq_validator.py --input procurement_pipeline_v2_results.csv --full-audit

    # Limit rows for testing
    python procurement_groq_validator.py --input procurement_pipeline_v2_results.csv --limit 50

Output:
    Same CSV with two extra columns: groq_verdict, groq_final_group
    groq_verdict  = CONFIRMED / CORRECTED / SKIPPED
    groq_final_group = final classification after Groq (may differ from predicted_group)

Requirements:
    pip install groq --break-system-packages
    Set env variable: GROQ_API_KEY=your_key_here
"""

import argparse
import csv
import os
import time
from pathlib import Path

# =============================================================================
# GROQ CLIENT
# =============================================================================

def get_groq_client():
    try:
        from groq import Groq
    except ImportError:
        print("groq not installed — run: pip install groq --break-system-packages")
        raise

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise ValueError(
            "GROQ_API_KEY environment variable not set.\n"
            "Get a free key at https://console.groq.com and set it:\n"
            "  Windows: set GROQ_API_KEY=your_key\n"
            "  Linux/Mac: export GROQ_API_KEY=your_key"
        )
    return Groq(api_key=api_key)


# =============================================================================
# PROMPT
# Focused on the two jobs: verify CONSULTING, rescue from NON-CONSULTING.
# Tight and unambiguous — no room for the LLM to waffle.
# =============================================================================

SYSTEM_PROMPT = """You are a procurement classification expert for international development tenders.

Your task is to classify a tender title into exactly ONE of these four groups:

CONSULTING — intellectual/advisory services: studies, evaluations, audits, technical assistance,
  capacity building, policy advice, strategy, governance reform, institutional strengthening,
  program management, monitoring & evaluation, digital transformation advisory,
  health system strengthening programs, education reform programs, social protection programs,
  any World Bank or development bank program that involves advisory/analytical work.

WORKS — physical construction or infrastructure: roads, buildings, water networks,
  irrigation canals, drilling, rehabilitation of physical assets, solar installation,
  civil works, any physical construction contract.

GOODS — supply and delivery of physical items: equipment, vehicles, medicines,
  seeds, fertilizers, IT hardware, furniture, materials, consumables.

NON-CONSULTING — outsourced operational services with NO intellectual deliverable:
  catering, hotel accommodation, cleaning, security guarding, vehicle hire,
  freight forwarding, translation/interpretation, printing, event management,
  travel management, insurance, maintenance contracts, internet/telecom services.

CRITICAL RULES:
- A program title like "X Governance Strengthening Project" or "X Digital Transformation Project"
  from a development bank is CONSULTING — it involves advisory and analytical work.
- "Service delivery" in a program name does NOT make it NON-CONSULTING.
- "Connectivity" in a program name does NOT make it NON-CONSULTING.
- "Transport sector reform" is CONSULTING. "Road construction" is WORKS.
- Only classify as NON-CONSULTING if the title clearly describes outsourced operational services
  (catering, cleaning, security, hotel, vehicle hire, etc.).

Reply with ONLY the group name: CONSULTING, WORKS, GOODS, or NON-CONSULTING.
No explanation. No punctuation. Just the group name."""


def classify_with_groq(client, title: str, model: str = "llama-3.1-8b-instant") -> str:
    """Call Groq and return the classification. Returns 'ERROR' on failure."""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": f'Classify this tender title:\n"{title}"'},
            ],
            temperature=0.0,
            max_tokens=10,
        )
        result = response.choices[0].message.content.strip().upper()
        # Normalise — only accept valid group names
        valid = {"CONSULTING", "WORKS", "GOODS", "NON-CONSULTING"}
        if result in valid:
            return result
        # Handle partial matches
        for v in valid:
            if v in result:
                return v
        return "ERROR"
    except Exception as e:
        print(f"  Groq error: {e}")
        return "ERROR"


# =============================================================================
# MAIN RUNNER
# =============================================================================

def run(
    input_path: str,
    output_path: str = None,
    full_audit: bool = False,
    limit: int = None,
    model: str = "llama-3.1-8b-instant",
    delay: float = 0.2,   # seconds between calls — stay within free tier rate limit
):
    input_path  = Path(input_path)
    output_path = Path(output_path) if output_path else input_path.with_stem(input_path.stem + "_groq")

    # Read input CSV
    with open(input_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Loaded {len(rows)} rows from {input_path.name}")

    # Decide which rows to send to Groq
    if full_audit:
        to_validate = [r for r in rows if r["predicted_group"] in ("CONSULTING", "NON-CONSULTING")]
        mode = "FULL AUDIT"
    else:
        to_validate = [r for r in rows if r.get("needs_groq") == "YES"]
        mode = "FLAGGED ONLY"

    if limit:
        to_validate = to_validate[:limit]

    print(f"Mode: {mode} — {len(to_validate)} rows to validate via Groq ({model})")

    if not to_validate:
        print("Nothing to validate. Run with --full-audit to process all CONSULTING/NON-CONSULTING rows.")
        return

    client = get_groq_client()

    # Index rows by title for fast lookup (titles may not be unique — process all)
    validated_titles = {}
    for i, row in enumerate(to_validate):
        title = row["title"]
        predicted = row["predicted_group"]

        if i % 10 == 0:
            print(f"  [{i+1}/{len(to_validate)}] processing...")

        groq_result = classify_with_groq(client, title, model=model)

        if groq_result == "ERROR":
            verdict   = "ERROR"
            final_grp = predicted
        elif groq_result == predicted:
            verdict   = "CONFIRMED"
            final_grp = predicted
        else:
            verdict   = "CORRECTED"
            final_grp = groq_result
            print(f"    CORRECTED: '{title[:60]}' {predicted} -> {groq_result}")

        validated_titles[title] = (verdict, final_grp)

        if delay > 0:
            time.sleep(delay)

    # Write output CSV — add groq columns to every row
    fieldnames = list(rows[0].keys()) + ["groq_verdict", "groq_final_group"]

    confirmed = corrected = skipped = errors = 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            title = row["title"]
            if title in validated_titles:
                verdict, final_grp = validated_titles[title]
                row["groq_verdict"]      = verdict
                row["groq_final_group"]  = final_grp
                if verdict == "CONFIRMED": confirmed += 1
                elif verdict == "CORRECTED": corrected += 1
                elif verdict == "ERROR": errors += 1
            else:
                row["groq_verdict"]      = "SKIPPED"
                row["groq_final_group"]  = row["predicted_group"]
                skipped += 1
            writer.writerow(row)

    # Summary
    total_validated = len(to_validate)
    print()
    print("=" * 55)
    print(f"GROQ VALIDATION COMPLETE — {total_validated} rows processed")
    print("=" * 55)
    print(f"  CONFIRMED  (Groq agreed)    : {confirmed}")
    print(f"  CORRECTED  (Groq disagreed) : {corrected}")
    print(f"  ERRORS     (API failures)   : {errors}")
    print(f"  SKIPPED    (not validated)  : {skipped}")
    if total_validated > 0:
        accuracy = (confirmed / total_validated) * 100
        print(f"  Agreement rate             : {accuracy:.1f}%")
    print()
    print(f"Results saved to: {output_path}")

    # Show all corrections
    if corrected > 0:
        print(f"\nAll corrections made by Groq:")
        for row in rows:
            if row.get("groq_verdict") == "CORRECTED":
                print(f"  {row['predicted_group']:<16} -> {row['groq_final_group']:<16} | {row['title'][:65]}")


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Layer 3 — Groq CONSULTING validator. Run after procurement_group_v2.py."
    )
    parser.add_argument("--input",       required=True,  help="Input CSV from procurement_group_v2.py")
    parser.add_argument("--output",      default=None,   help="Output CSV path (default: input_groq.csv)")
    parser.add_argument("--full-audit",  action="store_true",
                        help="Validate ALL CONSULTING + NON-CONSULTING rows, not just flagged ones")
    parser.add_argument("--limit",       type=int, default=None, help="Max rows to send to Groq")
    parser.add_argument("--model",       default="llama-3.1-8b-instant",
                        help="Groq model (default: llama-3.1-8b-instant). "
                             "Alternatives: mixtral-8x7b-32768, llama3-70b-8192")
    parser.add_argument("--delay",       type=float, default=0.2,
                        help="Seconds between API calls (default: 0.2 — respects free tier limits)")
    args = parser.parse_args()

    run(
        input_path  = args.input,
        output_path = args.output,
        full_audit  = args.full_audit,
        limit       = args.limit,
        model       = args.model,
        delay       = args.delay,
    )