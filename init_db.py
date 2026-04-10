from db import engine
from models import Base
import os
from db import engine
print(engine)
print("DATABASE_URL =", os.environ.get("DATABASE_URL"))

def init_db():
    print("Creating tables in PostgreSQL...")
    Base.metadata.create_all(engine)
    print("Done.")

if __name__ == "__main__":
    init_db()