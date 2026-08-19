import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from core.database import close_mongo_connection, connect_to_mongo
from core.routers.auth import router as auth_router
from core.routers.chat import limiter
from core.routers.chat import router as chat_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_to_mongo()
    yield
    await close_mongo_connection()


app = FastAPI(
    title="Fata AI Ultra Core API",
    description="Engine na Fata AI mai sarrafa Rubutu, Binciken Intanet, Kera Hotuna (Flux), da Aika Muryar Sauti (Voice TTS).",
    version="3.0.0",
    lifespan=lifespan,
)

# Tsaron Yawan Saƙo (Rate Limiter Setup)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Tsarin CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Sanya Routers
app.include_router(auth_router)
app.include_router(chat_router)


@app.get("/api/health", tags=["Health Check"])
async def health_check():
    return {
        "status": "Online",
        "system": "Fata AI Ultra Core Engine",
        "features": [
            "Qwen / Llama 3.3 Chat & AI Tutor",
            "Flux Image Generation",
            "Voice Synthesis (Whisper & TTS)",
            "Code Interpreter Engine",
            "PDF Document Analysis",
        ],
        "year": "2026",
        "message": "Fata AI Backend yana aiki lami lafiya!",
    }


@app.get("/", include_in_schema=False)
@app.get("/app", include_in_schema=False)
async def serve_frontend():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return {
        "status": "Online",
        "message": "Fata AI Backend yana aiki lami lafiya.",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)