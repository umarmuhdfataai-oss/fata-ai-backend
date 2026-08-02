import json
import asyncio
import os
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

# Amfani da Groq SDK
from groq import Groq

from core.security import get_current_user
from core.database import get_chat_collection

router = APIRouter(prefix="/chat", tags=["AI Chat Engine"])

# Configure Groq Client
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

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
            detail="GROQ_API_KEY is not configured on the server."
        )

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"

    user_query = req.message.strip()

    # MongoDB Chat History Retrieve
    chat_collection = get_chat_collection()
    
    # Cikakken bayani mai karfi domin bai wa Llama 3.3 damar bada cikakkun bayanai na yanzu (2026)
    system_instruction = (
        "Sunanka Fata AI, babban mataimakin fasaha wanda aka kirkira domin taimakon masu amfani da shi a duniya. "
        "Amsa duk tambayoyi cikin harshen Hausa ko Turanci gwargwadon yadda aka tambaye ka, cikin hikima, sauri, da inganci sosai kamar Gemini. "
        "Yanzu muna cikin shekara ta 2026. Ka kasance mai amfani da iliminka mai zurfi wajen bada ingantattun amsoshi masu daidai da gaskiyar lokaci."
    )

    messages = [
        {
            "role": "system",
            "content": system_instruction
        }
    ]
    
    if chat_collection is not None:
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat and "messages" in existing_chat:
            for msg in existing_chat["messages"][-6:]:  # Keep last 6 context messages
                role = "user" if msg["role"] == "user" else "assistant"
                messages.append({"role": role, "content": msg["content"]})

    # Add current prompt
    messages.append({"role": "user", "content": user_query})

    async def event_generator():
        full_assistant_response = ""
        try:
            # Amfani da llama-3.3-70b-versatile don samun ingantattun amsoshi masu ƙarfi
            response_stream = await asyncio.to_thread(
                client.chat.completions.create,
                model="llama-3.3-70b-versatile",
                messages=messages,
                stream=True
            )

            for chunk in response_stream:
                if chunk.choices[0].delta.content:
                    text_chunk = chunk.choices[0].delta.content
                    full_assistant_response += text_chunk
                    
                    payload = json.dumps({"content": text_chunk})
                    yield f"data: {payload}\n\n"
                    await asyncio.sleep(0.01)

            yield "data: [DONE]\n\n"

            # Save chat to MongoDB
            if chat_collection is not None:
                new_user_msg = {"role": "user", "content": user_query}
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
            err_payload = json.dumps({"content": f"⚠️ Kuskure daga Groq Engine: {str(e)}"})
            yield f"data: {err_payload}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")