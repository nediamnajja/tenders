# """
# back/main.py
# ==========================
# FastAPI application entry point.

# Run:
#     cd C:\projects\tenders\back
#     uvicorn main:app --reload --port 8000
# """

# import os
# from contextlib import asynccontextmanager

# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware

# from back.routers import sgd
# from database import engine, SessionLocal
# from models.db_models import Base, PlatformUser
# from routers import auth, tenders
# from routers.auth import hash_password
# from routers.sgd import router as sgd_router



# # =============================================================================
# # STARTUP
# # =============================================================================

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     Base.metadata.create_all(bind=engine)
#     _seed_admin()
#     yield


# def _seed_admin():
#     admin_email = os.environ.get("ADMIN_EMAIL", "admin@kpmg.com")
#     admin_password = os.environ.get("ADMIN_PASSWORD", "changeme123")

#     if len(admin_password.encode("utf-8")) > 72:
#         raise ValueError("ADMIN_PASSWORD must be 72 bytes or shorter because bcrypt has a 72-byte limit.")

#     db = SessionLocal()
#     try:
#         if db.query(PlatformUser).count() == 0:
#             admin = PlatformUser(
#                 email=admin_email,
#                 full_name="Admin",
#                 hashed_password=hash_password(admin_password),
#                 role="admin",
#             )
#             db.add(admin)
#             db.commit()
#             print(f"Admin user created: {admin_email}")
#             print("Change the password after first login!")
#     finally:
#         db.close()


# #=============================================================================
# # APP
# # =============================================================================
 
# app = FastAPI(
#     title       = "KPMG Tender Intelligence Platform",
#     description = "API for the KPMG tender recommendation and management platform.",
#     version     = "1.0.0",
#     lifespan    = lifespan,
# )
 
# # CORS — allow React dev server and local network access
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins     = ["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:3000"],
#     allow_credentials = True,
#     allow_methods     = ["*"],
#     allow_headers     = ["*"],
# )
 
# # Routers
# app.include_router(auth.router)
# app.include_router(tenders.router)


 
 
# @app.get("/health")
# def health():
#     return {"status": "ok", "service": "KPMG Tender Intelligence Platform"}

"""
back/main.py
==========================
FastAPI application entry point.

Run:
    cd C:\projects\tenders\back
    uvicorn main:app --reload --port 8000
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from back.database import engine, SessionLocal
from back.models.db_models import Base, PlatformUser
from back.routers import auth, tenders
from back.routers.auth import hash_password
from back.routers.sgd import router as sgd_router


# =============================================================================
# STARTUP
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    _seed_admin()
    yield


def _seed_admin():
    admin_email    = os.environ.get("ADMIN_EMAIL",    "admin@kpmg.com")
    admin_password = os.environ.get("ADMIN_PASSWORD", "changeme123")

    if len(admin_password.encode("utf-8")) > 72:
        raise ValueError(
            "ADMIN_PASSWORD must be 72 bytes or shorter (bcrypt limit)."
        )

    db = SessionLocal()
    try:
        if db.query(PlatformUser).count() == 0:
            admin = PlatformUser(
                email           = admin_email,
                full_name       = "Admin",
                hashed_password = hash_password(admin_password),
                role            = "admin",
            )
            db.add(admin)
            db.commit()
            print(f"  ✅ Admin user created: {admin_email}")
            print("  ⚠️  Change the password after first login!")
    finally:
        db.close()


# =============================================================================
# APP
# =============================================================================

app = FastAPI(
    title       = "KPMG Tender Intelligence Platform",
    description = "API for the KPMG tender recommendation and management platform.",
    version     = "1.0.0",
    lifespan    = lifespan,
    docs_url    = "/",
    redoc_url   = "/redoc",
    openapi_url = "/openapi.json",
)

# CORS — allow React dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins     = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
    ],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# Routers
app.include_router(auth.router)
app.include_router(tenders.router)
app.include_router(sgd_router)


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/health")
def health():
    return {"status": "ok", "service": "KPMG Tender Intelligence Platform"}