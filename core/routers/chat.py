import asyncio
import json
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse

from google import genai
from google.genai import types

from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/chat", tags=["AI Chat Engine (Gemini Ultra Core)"])

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

@router.post("/stream")
async def stream_chat(
    message: str = Form(""),
    session_id: Optional[str] = Form(None),
    model: Optional[str] = Form("gemini-2.5-flash"),
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
      
    # Umarni na musamman don amsa tambaya kai tsaye kawai
    system_prompt = (
        "ZAMANI DA SHEKARA: Yanzu muna shekarar 2026 ne.\n"
        "UMARNI MAI MUHIMMANCI:\n"
        "1. Ka ba da amsar tambayar da aka yi maka KAI TSAYE.\n"
        "2. KAR KA MAIMAITA gaisuwa, gabatar da kanka, ko cewa 'Ni ne Fata AI...' a duk lokacin da aka yi maka tambaya.\n"
        "3. Idan an yi gaisuwa kawai, za ka iya amsawa a takaice. Amma idan tambaya ce ta ilimi, labarai, ko bayani, JE KAI TSAYE ZUWA AMSAR TAMBAYAR.\n"
        "4. Amsa cikin harshen Hausa mai kyau, fahimta, da inganci."
    )

    async def event_generator():
        full_assistant_response = ""
        try:
            target_model = "gemini-2.5-flash"
            if model and "pro" in model.lower():
                target_model = "gemini-2.5-pro"

            chat_collection = get_chat_collection()
            history_contents = []
            
            # Dauko tarihin tattaunawa daga MongoDB
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

            # Sarrafa hoto/fayil idan an ɗora
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

            # Stream amsar kai tsaye daga Gemini API
            response = await client.aio.models.generate_content_stream(
                model=target_model,
                contents=history_contents,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.3,
                )
            )

            async for chunk in response:
                if chunk.text:
                    full_assistant_response += chunk.text
                    payload = json.dumps({"content": chunk.text})
                    yield f"data: {payload}\n\n"

            yield "data: [DONE]\n\n"

            # Adana tattaunawar a MongoDB
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