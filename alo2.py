import os
import pandas as pd
from sqlalchemy import text
from db import get_session

tables = ["organisations", "contacts", "tenders"]

with get_session() as session:
    xlsx_path = os.path.abspath("database_clean2.xlsx")
    csv_path = os.path.abspath("tenders_notice_text1.csv")

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        for table in tables:
            df = pd.read_sql(text(f"SELECT * FROM {table}"), session.bind)

            print(f"\nTABLE: {table}")
            print("Columns:", list(df.columns))
            print("Rows:", len(df))

            if table == "tenders":
                if "notice_text" in df.columns:
                    print("notice_text column found")
                    print("Non-null notice_text rows:", df["notice_text"].notna().sum())

                    df_notice = df[["id", "notice_text"]].copy()
                    df_notice.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    print("CSV saved to:", csv_path)
                else:
                    print("notice_text column NOT found")

                df = df.drop(columns=["notice_text"], errors="ignore")

            df.to_excel(writer, sheet_name=table, index=False)

print("Excel saved to:", xlsx_path)
print("Done")