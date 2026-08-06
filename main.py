import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from core.routers import chat
from core.database import connect_to_mongo, close_mongo_connection


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Action lokacin Startup
    await connect_to_mongo()
    yield
    # Action lokacin Shutdown
    await close_mongo_connection()


app = FastAPI(
    title="Fata AI Ultra Core API",
    description="Engine na Fata AI mai sarrafa Rubutu, Binciken Intanet, Kera Hotuna (Imagen 3), da Aika Muryar Sauti (Voice TTS).",
    version="3.0.0",
    lifespan=lifespan
)

# 2. CORS MANAGEMENT
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3. ROUTERS
app.include_router(chat.router, prefix="/api/v2")


# 4. HEALTH CHECK & FRONTEND
@app.get("/api/health", tags=["Health Check"])
async def health_check():
    return {
        "status": "Online",
        "system": "Fata AI Ultra Core Engine",
        "features": [
            "Gemini Chat & AI Tutor", 
            "Google Live Search", 
            "Imagen 3 Generation", 
            "Voice Synthesis (TTS)"
        ],
        "year": "2026",
        "message": "Fata AI Backend yana aiki lami lafiya!"
    }


@app.get("/", include_in_schema=False)
@app.get("/app", include_in_schema=False)
async def serve_frontend():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return {
        "status": "Online",
        "message": "Fata AI Backend yana aiki lami lafiya."
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)