import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.database import connect_to_mongo, close_mongo_connection
from routers import auth, chat, image, files, live

# Database Lifespan Handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("⚡ Starting Fata AI Core Engine...")
    try:
        await connect_to_mongo()
        print("🚀 Connected to MongoDB cluster successfully.")
    except Exception as e:
        print(f"🚨 Database connection error: {str(e)}")
    yield
    print("🛑 Shutting down Fata AI Core Engine...")
    try:
        await close_mongo_connection()
        print("💤 Database connections closed safely.")
    except Exception as e:
        print(f"🚨 Database shutdown error: {str(e)}")

# Initialize FastAPI
app = FastAPI(
    title="Fata AI Ultra Core Engine",
    description="Next-Generation Enterprise AI Architecture.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)

# CORS Security Layer Setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Root Endpoint
@app.get("/", tags=["System Health"])
async def root_health_check():
    return {
        "status": "online",
        "engine": "Fata AI Ultra Core",
        "version": "2.0.0",
        "author": "Fakruddeen",
        "message": "Welcome to the central node of Fata AI."
    }

# Route Integrations
try:
    app.include_router(auth.router, prefix="/api/v2")
    app.include_router(chat.router, prefix="/api/v2")
    app.include_router(image.router, prefix="/api/v2")
    app.include_router(files.router, prefix="/api/v2")
    app.include_router(live.router, prefix="/api/v2")
    print("✅ Routers loaded successfully.")
except Exception as e:
    print(f"🚨 Router integration error: {str(e)}")