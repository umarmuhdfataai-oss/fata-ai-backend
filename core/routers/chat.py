import asyncio
import json
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse

from google import genai
from google.genai import types

# Correct Imports
from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/chat", tags=["AI Chat Engine (Gemini Ultra Core)"])

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

@router.post("/stream")
async def stream_chat(
    message: str = Form(""),
    session_id: Optional[str] = Form(None),
    model: Optional[str] = Form("gemini-3.6-flash"),
    file: Optional[UploadFile] = File(None),
    current_user: dict = Depends(get_current_user)
):
    if not message and not file:
        raise HTTPException(status_code=400, detail="Saƙo ko fayil yana buƙatar kasancewa.")

    if not client:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY ba a tsara shi ba.")

    user_email = current_user.get("sub", "guest_user")
    session_id = session_id if session_id else "default_session"
    user_query = message.strip() if message else ""
      
    system_prompt = (
        "ZAMANI DA SHEKARA: Yanzu muna shekarar 2026 ne.\n"
        "KAI WANE NE: Kai ne 'Fata AI' mai amfani da babbar manhajar Google Gemini Ultra Core. "
        "Kuna da babban tunani, zurfin fahimta, damar riƙe dogon tarihi (Long-context memory), da karanta hotuna/fayiloli.\n\n"
        "TSARIN AMSA SAKO (INSTRUCTIONS):\n"
        "1. Yi amfani da dukkan bayanan da ke cikin tarihin tattaunawarku da mai amfani don ba da amsa mai kyau.\n"
        "2. Idan mai amfani ya tura hoto ko fayil, bincika hoton da kyau sannan ka ba da bayani a kansa.\n"
        "3. Idan an yi tambaya game da abubuwan da ke faruwa a duniyar yanzu, yi amfani da kayan bincike don gano ainihin abin da ke faruwa.\n"
        "4. Amsa cikin harshen Hausa mai daɗi, inganci, fahimta, da girmamawa."
    )

    async def event_generator():
        full_assistant_response = ""
        try:
            target_model = "gemini-3.6-flash"
            if model and "pro" in model.lower():
                target_model = "gemini-3.6-pro"

            chat_collection = get_chat_collection()
            history_contents = []
            
            # 1. Dauko dukkan tarihin tattaunawa daga MongoDB
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

            # 2. Sarrafa Hoto/Fayil idan an tura
            current_user_parts = []
            if file:
                file_bytes = await file.read()
                mime_type = file.content_type
                current_user_parts.append(
                    types.Part.from_bytes(data=file_bytes, mime_type=mime_type)
                )
            
            if user_query:
                current_user_parts.append(types.Part.from_text(text=user_query))

            history_contents.append(types.Content(role="user", parts=current_user_parts))

            # 3. Kayan Bincike na Google Search (Correct Syntax)
            tools_list = [{"google_search": {}}, {"code_execution": {}}]

            # 4. Aika saƙo ta hanyar Async Stream
            response = await client.aio.models.generate_content_stream(
                model=target_model,
                contents=history_contents,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.4,
                    tools=tools_list
                )
            )

            async for chunk in response:
                if chunk.text:
                    full_assistant_response += chunk.text
                    payload = json.dumps({"content": chunk.text})
                    yield f"data: {payload}\n\n"

            yield "data: [DONE]\n\n"

            # 5. Adana a MongoDB
            if chat_collection is not None:
                new_user_msg = {"role": "user", "content": user_query or "[Hoto/Fayil]"}
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