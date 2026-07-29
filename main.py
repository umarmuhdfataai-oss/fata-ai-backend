import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis

from core.database import connect_to_mongo, close_mongo_connection
from routers import auth, chat, image, files, live

# Create redis_client to be accessible globally
redis_client = None

# Database Lifespan Handler: MongoDB and Redis
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    print("⚡ Starting Fata AI Core Engine...")
    
    # 1. MongoDB Connection
    try:
        await connect_to_mongo()
        print("🚀 Connected to MongoDB cluster successfully.")
    except Exception as e:
        print(f"🚨 MongoDB connection error: {str(e)}")
        
    # 2. Redis Connection (yayi amfani da REDIS_URI daga Render)
    try:
        redis_uri = os.getenv("REDIS_URI")
        if redis_uri:
            print("🔄 Connecting to Redis cluster...")
            redis_client = aioredis.from_url(redis_uri, decode_responses=True)
            await redis_client.ping()
            print("🚀 Connected to Redis successfully.")
        else:
            print("⚠️ REDIS_URI variable is missing in environment.")
    except Exception as e:
        print(f"🚨 Redis connection error: {str(e)}")

    yield
    
    # Shutdown logic
    print("🛑 Shutting down Fata AI Core Engine...")
    try:
        await close_mongo_connection()
        if redis_client:
            await redis_client.close()
        print("💤 Database connections closed safely.")
    except Exception as e:
        print(f"🚨 Shutdown error: {str(e)}")

# Initialize FastAPI app
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

# Health Check Endpoint
@app.get("/", tags=["System Health"])
async def root_health_check():
    groq_configured = bool(os.getenv("GROQ_API_KEY"))
    redis_status = "connected" if redis_client else "disconnected"

    return {
        "status": "online",
        "engine": "Fata AI Ultra Core",
        "version": "2.0.0",
        "author": "Fakruddeen",
        "message": "Welcome to the central node of Fata AI.",
        "services": {
            "groq_api": "active" if groq_configured else "missing",
            "redis_cache": redis_status
        }
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