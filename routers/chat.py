import json
import asyncio
import os
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

# Sabon SDK na Google GenAI
from google import genai
from google.genai import types

from core.security import get_current_user
from core.database import get_chat_collection

router = APIRouter(prefix="/chat", tags=["AI Chat Engine"])

# Configure Google Gemini Client
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None

@router.post("/stream")
async def stream_chat(
    req: ChatRequest,
    current_user: dict = Depends(get_current_user)
):
    if not req.message or not req.message.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Message cannot be empty."
        )

    if not client:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="GEMINI_API_KEY is not configured on the server."
        )

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"

    # MongoDB Chat History Retrieve
    chat_collection = get_chat_collection()
    gemini_contents = []
    
    if chat_collection is not None:
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat and "messages" in existing_chat:
            for msg in existing_chat["messages"][-6:]:  # Keep last 6 context messages
                role_map = "user" if msg["role"] == "user" else "model"
                gemini_contents.append(
                    types.Content(
                        role=role_map,
                        parts=[types.Part.from_text(text=msg["content"])]
                    )
                )

    # Add current prompt
    gemini_contents.append(
        types.Content(
            role="user",
            parts=[types.Part.from_text(text=req.message.strip())]
        )
    )

    # System instruction & Config
    system_instruction = (
        "Ni ne Fata AI, mataimakin mai amfani mai amfani da Gemini Engine. "
        "Amsa tambayoyi cikin harshen Hausa ko Turanci a sauƙaƙe da kiyaye sararin kalmomi (spaces)."
    )
    
    config = types.GenerateContentConfig(
        system_instruction=system_instruction
    )

    async def event_generator():
        full_assistant_response = ""
        try:
            # Amfani da gemini-2.5-flash dai-dai da account ɗinka
            response_stream = await asyncio.to_thread(
                client.models.generate_content_stream,
                model='gemini-2.5-flash',
                contents=gemini_contents,
                config=config
            )

            for chunk in response_stream:
                if chunk.text:
                    text_chunk = chunk.text
                    full_assistant_response += text_chunk
                    
                    payload = json.dumps({"content": text_chunk})
                    yield f"data: {payload}\n\n"
                    await asyncio.sleep(0.01)

            yield "data: [DONE]\n\n"

            # Save chat to MongoDB
            if chat_collection is not None:
                new_user_msg = {"role": "user", "content": req.message.strip()}
                new_ai_msg = {"role": "assistant", "content": full_assistant_response}
                
                await chat_collection.update_one(
                    {"_id": session_id},
                    {
                        "$set": {"user_email": user_email},
                        "$push": {"messages": {"$each": [new_user_msg, new_ai_msg]}}
                    },
                    upsert=True
                )

        except Exception as e:
            print(f"🚨 Streaming Error: {str(e)}")
            err_payload = json.dumps({"content": f"⚠️ Kuskure daga Gemini Engine: {str(e)}"})
            yield f"data: {err_payload}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")