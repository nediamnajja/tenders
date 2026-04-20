"""
training/train_sector_classifier.py
=====================================
Trains a SetFit model for tender sector classification.

Uses the labeled CSV (title, sector) to fine-tune
paraphrase-multilingual-MiniLM-L12-v2 — the same model
already used for KeyBERT, so no new downloads needed.

Run:
    python training/train_sector_classifier.py
    python training/train_sector_classifier.py --data path/to/data.csv
    python training/train_sector_classifier.py --epochs 4 --test-size 0.2

Output:
    models/sector_classifier_setfit/   ← trained model saved here
    training/evaluation_report.txt     ← accuracy per sector

Dependencies:
    pip install setfit datasets scikit-learn
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  PATHS
# ─────────────────────────────────────────────────────────────────────────────

ROOT_DIR       = Path(__file__).resolve().parent.parent
DATA_PATH      = ROOT_DIR / "enricher" / "training_data.csv"
MODEL_OUT_DIR  = ROOT_DIR / "models" / "sector_classifier_setfit"
EVAL_OUT_PATH  = ROOT_DIR / "training" / "evaluation_report.txt"

# ─────────────────────────────────────────────────────────────────────────────
#  ALL 23 SECTORS — must match stage3_nlp.py exactly
# ─────────────────────────────────────────────────────────────────────────────

ALL_SECTORS = [
    "Digital Transformation",
    "Cybersecurity & Data Security",
    "Data, AI & Analytics",
    "Telecommunications",
    "Enterprise IT & Systems Implementation",
    "Energy & Utilities",
    "Construction & Infrastructure",
    "Transport & Logistics",
    "Water, Sanitation & Waste",
    "Agriculture & Food Security",
    "Environment & Climate",
    "Education & Training",
    "Health & Life Sciences",
    "Financial Services",
    "Government Reform & Public Administration",
    "Justice & Rule of Law",
    "Risk & Compliance",
    "Organizational Reform & HR Management",
    "Employment & Skills Development",
    "Business Strategy & Performance",
    "Marketing & Customer Experience",
    "Mining & Natural Resources",
    "Social Protection & Poverty Reduction",
]


# ─────────────────────────────────────────────────────────────────────────────
#  LOAD & VALIDATE DATA
# ─────────────────────────────────────────────────────────────────────────────

def load_data(csv_path: Path) -> tuple:
    """
    Load the training CSV, validate sector names, return (texts, labels).
    Skips rows with unrecognized sector names and warns about them.
    """
    import pandas as pd

    log.info("Loading training data from %s", csv_path)
    df = pd.read_csv(csv_path, on_bad_lines="skip")
    df = df.dropna(subset=["title", "sector"])
    df["title"]  = df["title"].str.strip()
    df["sector"] = df["sector"].str.strip()

    # Validate sector names
    unknown = set(df["sector"].unique()) - set(ALL_SECTORS)
    if unknown:
        log.warning("Unknown sectors (will be skipped): %s", unknown)
        df = df[df["sector"].isin(ALL_SECTORS)]

    # Log distribution
    log.info("Total examples after validation: %d", len(df))
    counts = df["sector"].value_counts()
    log.info("Sector distribution:")
    for sector, count in counts.items():
        flag = " ← LOW" if count < 8 else ""
        log.info("  %3d  %s%s", count, sector, flag)

    # Warn about sectors with very few examples
    for sector in ALL_SECTORS:
        count = counts.get(sector, 0)
        if count == 0:
            log.warning("Sector has ZERO examples — will not be learnable: %s", sector)
        elif count < 8:
            log.warning("Sector has only %d examples (recommend 8+): %s", count, sector)

    texts  = df["title"].tolist()
    labels = df["sector"].tolist()
    return texts, labels


# ─────────────────────────────────────────────────────────────────────────────
#  TRAIN
# ─────────────────────────────────────────────────────────────────────────────

def train(
    texts:     list[str],
    labels:    list[str],
    epochs:    int   = 3,
    test_size: float = 0.2,
    seed:      int   = 42,
) -> None:
    """
    Fine-tune SetFit on the labeled tender data.

    Uses paraphrase-multilingual-MiniLM-L12-v2 as backbone —
    same model as KeyBERT, already cached on disk.
    """
    from setfit import SetFitModel, Trainer, TrainingArguments
    from datasets import Dataset
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import classification_report, accuracy_score
    import numpy as np

    log.info("Splitting data (test_size=%.0f%%)", test_size * 100)
    X_train, X_test, y_train, y_test = train_test_split(
        texts, labels,
        test_size=test_size,
        random_state=seed,
        stratify=labels,      # keep sector proportions in both splits
    )
    log.info("Train: %d  |  Test: %d", len(X_train), len(X_test))

    # Build HuggingFace datasets
    train_ds = Dataset.from_dict({"text": X_train, "label": y_train})
    test_ds  = Dataset.from_dict({"text": X_test,  "label": y_test})

    # Load model — uses multilingual MiniLM already on disk
    log.info("Loading SetFit model (paraphrase-multilingual-MiniLM-L12-v2)...")
    model = SetFitModel.from_pretrained(
        "paraphrase-multilingual-MiniLM-L12-v2",
        labels=ALL_SECTORS,
    )

    # Training arguments
    args = TrainingArguments(
        batch_size=16,
        num_epochs=epochs,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        seed=seed,
    )

    # Custom metric — accuracy
    def compute_metrics(y_pred, y_test):
        return {"accuracy": accuracy_score(y_test, y_pred)}

    # Trainer
    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=train_ds,
        eval_dataset=test_ds,
        metric=compute_metrics,
    )

    log.info("Starting training (%d epochs)...", epochs)
    trainer.train()
    log.info("Training complete.")

    # ── Evaluate ──────────────────────────────────────────────────────────────
    log.info("Evaluating on test set...")
    y_pred = model.predict(X_test)

    # Overall accuracy
    acc = accuracy_score(y_test, y_pred)
    log.info("Overall accuracy: %.1f%%", acc * 100)

    # Per-sector report
    report = classification_report(
        y_test, y_pred,
        labels=ALL_SECTORS,
        zero_division=0,
    )
    log.info("Per-sector report:\n%s", report)

    # Save evaluation report to file
    EVAL_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(EVAL_OUT_PATH, "w") as f:
        f.write(f"Overall accuracy: {acc * 100:.1f}%\n\n")
        f.write(report)
    log.info("Evaluation report saved to %s", EVAL_OUT_PATH)

    # ── Save model ────────────────────────────────────────────────────────────
    MODEL_OUT_DIR.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(str(MODEL_OUT_DIR))
    log.info("Model saved to %s", MODEL_OUT_DIR)

    # Save sector list alongside model for reference
    with open(MODEL_OUT_DIR / "sectors.json", "w") as f:
        json.dump(ALL_SECTORS, f, indent=2, ensure_ascii=False)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print(f"  Training complete")
    print(f"  Overall accuracy : {acc * 100:.1f}%")
    print(f"  Model saved to   : {MODEL_OUT_DIR}")
    print(f"  Eval report      : {EVAL_OUT_PATH}")
    print("="*60)

    if acc < 0.70:
        log.warning(
            "Accuracy below 70%%. Consider adding more labeled examples "
            "for the weak sectors shown in the report above."
        )
    elif acc < 0.80:
        log.info(
            "Accuracy between 70-80%%. Good start. "
            "Add more examples for weak sectors to reach 80%%+."
        )
    else:
        log.info("Accuracy above 80%%. Ready for production use.")


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train SetFit sector classifier for tender NLP pipeline."
    )
    parser.add_argument(
        "--data", type=Path, default=DATA_PATH,
        help=f"Path to labeled CSV (default: {DATA_PATH})",
    )
    parser.add_argument(
        "--epochs", type=int, default=3,
        help="Number of training epochs (default: 3)",
    )
    parser.add_argument(
        "--test-size", type=float, default=0.2,
        help="Fraction of data to use for evaluation (default: 0.2)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    args = parser.parse_args()

    if not args.data.exists():
        log.error("Training data not found: %s", args.data)
        sys.exit(1)

    texts, labels = load_data(args.data)
    train(
        texts     = texts,
        labels    = labels,
        epochs    = args.epochs,
        test_size = args.test_size,
        seed      = args.seed,
    )