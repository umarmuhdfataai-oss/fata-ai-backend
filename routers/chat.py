import json
import asyncio
import os
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import google.generativeai as genai

from core.security import get_current_user
from core.database import get_chat_collection

router = APIRouter(prefix="/chat", tags=["AI Chat Engine"])

# Configure Google Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

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

    if not GEMINI_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="GEMINI_API_KEY is not configured on the server."
        )

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"

    # MongoDB Chat History Retrieve
    chat_collection = get_chat_collection()
    history_contents = []
    
    if chat_collection is not None:
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat and "messages" in existing_chat:
            for msg in existing_chat["messages"][-6:]:  # Keep last 6 context messages
                role = "user" if msg["role"] == "user" else "model"
                history_contents.append({
                    "role": role,
                    "parts": [msg["content"]]
                })

    # Add system instructions and current prompt
    system_instruction = (
        "Ni ne Fata AI, mataimakin mai amfani mai amfani da Gemini Engine. "
        "Amsa tambayoyi cikin harshen Hausa ko Turanci a sauƙaƙe da kiyaye sararin kalmomi (spaces)."
    )
    
    # An canza model_name zuwa gemini-2.5-flash wanda ke aiki da sabbin API keys
    model = genai.GenerativeModel(
        model_name='gemini-2.5-flash',
        system_instruction=system_instruction
    )

    # Append new user prompt
    history_contents.append({
        "role": "user",
        "parts": [req.message.strip()]
    })

    async def event_generator():
        full_assistant_response = ""
        try:
            # Generate content using Gemini Async Stream
            response = await asyncio.to_thread(
                model.generate_content,
                history_contents,
                stream=True
            )

            for chunk in response:
                if chunk.text:
                    text_chunk = chunk.text
                    full_assistant_response += text_chunk
                    
                    # Send payload in JSON format to safeguard formatting/spaces
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