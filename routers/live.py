import os
import datetime
import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from core.database import get_chat_collection

router = APIRouter(prefix="/live", tags=["Real-time Audio Engine"])

async def log_live_audio(session_id: str, user_email: str, event: str):
    """
    Adana logs na live audio socket a MongoDB.
    """
    chat_collection = get_chat_collection()
    if chat_collection is None:
        print("🚨 Live Audio DB Log Failure: Chat collection is not initialized.")
        return
    
    try:
        history = []
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat:
            history = existing_chat.get("messages", [])
        
        history.append({
            "role": "system", 
            "content": event, 
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        
        await chat_collection.update_one(
            {"_id": session_id},
            {
                "$set": {
                    "user_email": user_email,
                    "messages": history,
                    "chat_mode": "live_audio",
                    "title": "AI Live Audio Session",
                    "updated_at": datetime.datetime.now(datetime.timezone.utc)
                }
            },
            upsert=True
        )
    except Exception as e:
        print(f"🚨 Live Audio DB Log Error: {str(e)}")

@router.websocket("/ws")
async def live_audio_socket(websocket: WebSocket):
    await websocket.accept()
    api_key = os.environ.get("GEMINI_API_KEY")
    
    if not api_key:
        await websocket.close(code=1008, reason="GEMINI_API_KEY missing")
        return
        
    try:
        await websocket.send_text("Fata AI Live Stream Engine (Powered by Gemini) connected successfully.")
        
        # Adana log din session a background
        asyncio.create_task(log_live_audio("live_session", "guest_user", "Live Gemini session started."))
        
        while True:
            data = await websocket.receive_bytes()
            # Karbar bytes ba tare da dakatar da WebSocket ba
            asyncio.create_task(log_live_audio("live_session", "guest_user", f"Received {len(data)} bytes of streaming data."))
            
    except WebSocketDisconnect:
        print("⚡ Live Socket Client Disconnected gracefully.")
        asyncio.create_task(log_live_audio("live_session", "guest_user", "Live audio session disconnected."))
    except Exception as e:
        print(f"🚨 Live Audio Pipeline Error: {str(e)}")
        asyncio.create_task(log_live_audio("live_session", "guest_user", f"Error: {str(e)}"))
        await websocket.close(code=1011, reason=str(e))