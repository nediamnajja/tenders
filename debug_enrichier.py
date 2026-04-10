from dotenv import load_dotenv
load_dotenv()

import os
print("GROQ_API_KEY set:", bool(os.environ.get("GROQ_API_KEY")))
print("Key starts with:", os.environ.get("GROQ_API_KEY", "")[:8])

from db import init_db, get_session
from sqlalchemy import text

init_db()

with get_session() as s:
    rows = s.execute(text("""
        SELECT t.id, t.tender_id, t.title, length(t.notice_text) as nt_len
        FROM tenders t
        LEFT JOIN enriched_tenders e ON e.tender_id = t.id
        WHERE t.source_portal = 'worldbank'
          AND t.notice_text IS NOT NULL
          AND t.notice_text != ''
          AND e.id IS NULL
        LIMIT 5
    """)).fetchall()

print(f"\nPending tenders found: {len(rows)}")
for r in rows:
    print(f"  id={r[0]} notice_id={r[1]} notice_text_len={r[3]} title={r[2][:50]}")