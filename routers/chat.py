import asyncio
import json
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from google import genai
from google.genai import types
from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/chat", tags=["AI Chat Engine (Gemini 3.6)"])

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    model: Optional[str] = "gemini-3.6-flash"


@router.post("/stream")
async def stream_chat(
    req: ChatRequest,
    current_user: dict = Depends(get_current_user)
):
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="Saƙo ba zai iya kasancewa wofi ba.")

    if not client:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY ba a tsara shi ba.")

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"
    user_query = req.message.strip()
      
    system_prompt = (
        "Kai ne Gemini, babban mataimakin AI na Google. "
        "Yi amfani da salo da dabarun Gemini na asali wajen amsa tambayoyi cikin harshen Hausa mai kyau, gaskiya, amfani, da cikakken bayani. "
        "Amsa cikin tsari na gaskiya da kaifin tunani, tare da amfani da kwanan wata na yanzu (2026)."
    )

    async def event_generator():
        full_assistant_response = ""
        try:
            # Amfani da samfurin Gemini 3.6 
            target_model = "gemini-3.6-flash"
            if req.model and "pro" in req.model.lower():
                target_model = "gemini-3.6-pro"

            chat_collection = get_chat_collection()
            history_contents = []
            
            # 1. Dauko tarihin tattaunawa daga MongoDB
            if chat_collection is not None:
                session_data = await chat_collection.find_one({"_id": session_id})
                if session_data and "messages" in session_data:
                    for msg in session_data["messages"]:
                        role = "user" if msg["role"] == "user" else "model"
                        history_contents.append(
                            types.Content(
                                role=role,
                                parts=[types.Part.from_text(text=msg["content"])]
                            )
                        )

            # 2. Sanya sabon saƙon mai amfani
            history_contents.append(
                types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=user_query)]
                )
            )

            # 3. Tura saƙo zuwa Gemini 3.6 Engine
            response = await asyncio.to_thread(
                client.models.generate_content_stream,
                model=target_model,
                contents=history_contents,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.7,
                )
            )

            for chunk in response:
                if chunk.text:
                    full_assistant_response += chunk.text
                    payload = json.dumps({"content": chunk.text})
                    yield f"data: {payload}\n\n"

            yield "data: [DONE]\n\n"

            # 4. Adana sakamako a MongoDB
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
            err_payload = json.dumps({"content": f"⚠️ Kuskure daga Gemini Engine: {str(e)}"})
            yield f"data: {err_payload}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")