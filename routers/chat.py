import json
import asyncio
import os
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
from groq import AsyncGroq

from core.security import get_current_user
from core.database import get_chat_collection

router = APIRouter(prefix="/chat", tags=["AI Chat Engine"])

# Initialize Groq Client
groq_client = AsyncGroq(api_key=os.getenv("GROQ_API_KEY"))

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

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"

    # MongoDB Chat History Retrieve
    chat_collection = get_chat_collection()
    history_messages = []
    
    if chat_collection is not None:
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat and "messages" in existing_chat:
            for msg in existing_chat["messages"][-6:]:  # Keep last 6 context messages
                history_messages.append({"role": msg["role"], "content": msg["content"]})

    # System Instructions tare da ainihin Kwanan Wata da Shekara (2026)
    system_prompt = {
        "role": "system",
        "content": (
            "Ni ne Fata AI, mataimaki mai hikima da ke amsa tambayoyi cikin Hausa da Turanci daidai cikin amana. "
            "A yau shekara ita ce 2026. Tabbatar dukkan amsoshin da kake bayarwa game da labarai, wasanni, da abubuwan da suka faru "
            "suna dace da shekarar 2026 kuma ka ware kalmomi da sarari (space) yadda ya kamata."
        )
    }
    
    messages_payload = [system_prompt] + history_messages + [{"role": "user", "content": req.message.strip()}]

    async def event_generator():
        full_assistant_response = ""
        try:
            # Groq Streaming Call
            stream = await groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=messages_payload,
                stream=True
            )

            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    text_chunk = chunk.choices[0].delta.content
                    full_assistant_response += text_chunk
                    
                    # Sauya sabon layi (\n) kadai, ba tare da goge ko daya daga cikin spaces ba
                    clean_chunk = text_chunk.replace("\n", "\\n")
                    yield f"data: {clean_chunk}\n\n"

            yield "data: [DONE]\n\n"

            # Save completed interaction to MongoDB asynchronously
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
            yield f"data: ⚠️ Kuskure daga AI Engine: {str(e)}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")