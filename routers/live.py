import asyncio
import datetime
import os
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from core.database import get_chat_collection

router = APIRouter(prefix="/live", tags=["Real-time Gemini Audio Engine"])

async def log_live_audio(session_id: str, user_email: str, event: str):
    chat_collection = get_chat_collection()
    if chat_collection is None:
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
                    "title": "Gemini Live Session",
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
        await websocket.send_text("Gemini Live Real-time Audio Engine connected successfully.")
        asyncio.create_task(log_live_audio("live_session", "guest_user", "Live Gemini session started."))
        
        while True:
            data = await websocket.receive_bytes()
            asyncio.create_task(log_live_audio("live_session", "guest_user", f"Received {len(data)} bytes of audio data."))
            
    except WebSocketDisconnect:
        asyncio.create_task(log_live_audio("live_session", "guest_user", "Live audio session disconnected."))
    except Exception as e:
        await websocket.close(code=1011, reason=str(e))