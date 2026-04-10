import pandas as pd
from sqlalchemy import select
from db import get_session
from models import EnrichedTender

def export_to_excel(output_file="tender_texts2.xlsx", limit=None):
    with get_session() as session:
        stmt = select(
            EnrichedTender.tender_id,
            EnrichedTender.notice_id,
            EnrichedTender.notice_text_clean
        ).where(
            EnrichedTender.notice_text_clean.isnot(None)
        )

        if limit:
            stmt = stmt.limit(limit)

        results = session.execute(stmt).all()

    # Convert to DataFrame
    df = pd.DataFrame(results, columns=[
        "tender_id",
        "notice_id",
    
        "text"
    ])

    # Save to Excel
    df.to_excel(output_file, index=False)

    print(f"✅ Exported {len(df)} rows to {output_file}")


if __name__ == "__main__":
    export_to_excel(limit=50)  # start small for testing