from contextlib import asynccontextmanager
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis

from core.database import close_mongo_connection, connect_to_mongo
from routers import auth, chat, files, history, image, live

redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    print("⚡ Starting Gemini AI Core Engine...")

    try:
        await connect_to_mongo()
        print("🚀 Connected to MongoDB cluster successfully.")
    except Exception as e:
        print(f"🚨 MongoDB connection error: {str(e)}")

    try:
        redis_uri = os.getenv("REDIS_URI")
        if redis_uri:
            redis_client = aioredis.from_url(redis_uri, decode_responses=True)
            await redis_client.ping()
            print("🚀 Connected to Redis successfully.")
    except Exception as e:
        print(f"🚨 Redis connection error: {str(e)}")

    yield

    print("🛑 Shutting down Gemini Core Engine...")
    try:
        await close_mongo_connection()
        if redis_client:
            await redis_client.close()
    except Exception as e:
        print(f"🚨 Shutdown error: {str(e)}")

app = FastAPI(
    title="Gemini AI Core Backend Engine",
    description="Enterprise Native AI Architecture Powered by Gemini.",
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

@app.get("/", tags=["System Health"])
async def root_health_check():
    gemini_configured = bool(os.getenv("GEMINI_API_KEY"))
    redis_status = "connected" if redis_client else "disconnected"

    return {
        "status": "online",
        "engine": "Gemini AI Engine Native",
        "version": "3.0.0",
        "services": {
            "gemini_api": "active" if gemini_configured else "missing",
            "redis_cache": redis_status,
        },
    }

try:
    app.include_router(auth.router, prefix="/api/v2")
    app.include_router(chat.router, prefix="/api/v2")
    app.include_router(image.router, prefix="/api/v2")
    app.include_router(files.router, prefix="/api/v2")
    app.include_router(live.router, prefix="/api/v2")
    app.include_router(history.router, prefix="/api/v2")
    print("✅ Gemini Routers loaded successfully.")
except Exception as e:
    print(f"🚨 Router integration error: {str(e)}")