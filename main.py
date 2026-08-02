import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Shigar da Routers ɗinka daga hanyoyin core/routers
from core.routers import chat, auth  # Tabbatar kana da auth ko sauran routers idan akwai

app = FastAPI(
    title="Fata AI Ultra Core API",
    description="API Engine na Fata AI mai amfani da Google Gemini 3.6 Ultra Core, Real-time Search, da Vision Capabilities.",
    version="3.0.0"
)

# 1. SARAFA CORS (Cross-Origin Resource Sharing)
# Wannan yana bawa Frontend dinka (daga ko ina ko daga localhost) damar kiran Backend danka ba tare da samun 'CORS error' ba
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Zaka iya saita ainihin URL dinka a nan idan kana buƙata
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. HAƊA ROUTERS (ENDPOINTS)
app.include_router(chat.router, prefix="/api/v2")
# app.include_router(auth.router, prefix="/api/v2")  # Tuka wannan idan kana da auth router

# 3. ROOT ENDPOINT (GWADA BACKEND STATUS)
@app.get("/", tags=["Health Check"])
async def root():
    return {
        "status": "Online",
        "system": "Fata AI Ultra Core Engine",
        "powered_by": "Google Gemini 3.6",
        "year": "2026",
        "message": "Sannu da zuwa! Backend dinka yana aiki daidai tare da dukkan kayan aikin Gemini."
    }

# 4. DUBA IF INDEX.HTML YANA CIKIN FOLDER (Optional Static File Serving)
# Idan kaga dama zaka iya sanya index.html dinka a cikin folder daya da main.py don Render ya buɗe fuskarta kai tsaye.
if os.path.exists("index.html"):
    @app.get("/app", include_in_schema=False)
    async def serve_frontend():
        return FileResponse("index.html")

if __name__ == "__main__":
    import uvicorn
    # Aiki a komfutarka na gida (Local Development)
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)