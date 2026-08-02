import json
import asyncio
import os
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from google import genai
from google.genai import types
from core.security import get_current_user
from core.database import get_chat_collection

router = APIRouter(prefix="/chat", tags=["AI Chat Engine (Gemini)"])

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
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    if not client:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY is not configured.")

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"
    user_query = req.message.strip()
      
    system_prompt = (
        "Sunanka Fata AI, babban mataimakin fasaha kuma ƙwararren mai bincike da aka gina a kan Google Gemini. "
        "Amsa duk tambayoyin masu amfani cikin harshen Hausa mai daɗi, inganci, da cikakken bayani kamar yadda Gemini yake yi. "
        "Yanzu muna cikin shekara ta 2026. Ka kasance mai kaifi, mai fahimtar jigon tattaunawa ta baya, kuma ka tuna duk abin da aka tattauna a cikin wannan zance."
    )

    async def event_generator():
        full_assistant_response = ""
        try:
            target_model = "gemini-3.6-flash"
            if req.model and "pro" in req.model.lower():
                target_model = "gemini-3.6-pro"

            # 1. Ɗauko tsoffin saƙonnin tattaunawa (Chat History) daga MongoDB domin tuna zance
            chat_collection = get_chat_collection()
            history_contents = []
            
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

            # 2. Sanya sabon saƙon da mai amfani ya rubuta a ƙarshe
            history_contents.append(
                types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=user_query)]
                )
            )

            # 3. Tura dukkan tarihi da sabon saƙo zuwa ga Gemini
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

            # 4. Adana sabon saƙo da amsar AI a cikin MongoDB
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