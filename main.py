import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from core.routers import chat
from core.database import connect_to_mongo, close_mongo_connection

app = FastAPI(
    title="Fata AI Ultra Core API",
    description="API Engine na Fata AI mai amfani da Google Gemini 3.6 Ultra Core Engine.",
    version="3.0.0"
)

# Startup da Shutdown events
@app.on_event("startup")
async def startup_db_client():
    await connect_to_mongo()

@app.on_event("shutdown")
async def shutdown_db_client():
    await close_mongo_connection()

# 1. SARAFA CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. HAƊA ROUTER NA CHAT
app.include_router(chat.router, prefix="/api/v2")

# 3. ROOT ENDPOINT
@app.get("/", tags=["Health Check"])
async def root():
    return {
        "status": "Online",
        "system": "Fata AI Ultra Core Engine",
        "powered_by": "Google Gemini 3.6",
        "year": "2026",
        "message": "Fata AI Backend yana aiki lami lafiya!"
    }

# 4. SERVE FRONTEND
if os.path.exists("index.html"):
    @app.get("/app", include_in_schema=False)
    async def serve_frontend():
        return FileResponse("index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)