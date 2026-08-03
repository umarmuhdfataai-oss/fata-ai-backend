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
        "ZAMANI DA SHEKARA: Yanzu muna shekarar 2026 ne.\n\n"
        "UMARNI DA TSARIN ILIMI DOKI:\n"
        "1. KAI NE FATA AI: Wata babbar manhajar basira ta artificial intelligence mai zurfin ilimi, dabara, da fasaha matuka.\n"
        "2. Amsa tambayoyi daki-daki da zurfin bayani mai amfani, gaskiya, da kaifi.\n"
        "3. Yi amfani da sahihiyar Hausa mai inganci, bayyananniya, da kwarjini.\n"
        "4. Kar ka riƙa maimaita gaisuwa ko gabatar da kanka a kowane saƙo idan an riga an fara magana; faɗa kai tsaye cikin amsar tambayar."
    )

    async def event_generator():
        full_assistant_response = ""
        try:
            target_model = model.strip() if model else "gemini-3.6-flash"

            contents = []
            if file:
                file_bytes = await file.read()
                mime_type = file.content_type or "image/jpeg"
                contents.append(types.Part.from_bytes(data=file_bytes, mime_type=mime_type))

            if user_query:
                contents.append(user_query)

            # An cire Search Tool domin kiyaye API Rate Limit (Quota)
            config = types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.7
            )

            def generate():
                return client.models.generate_content_stream(
                    model=target_model,
                    contents=contents,
                    config=config
                )

            response_stream = await asyncio.to_thread(generate)

            for chunk in response_stream:
                if chunk.text:
                    full_assistant_response += chunk.text
                    yield f"data: {json.dumps({'content': chunk.text})}\n\n"
                    await asyncio.sleep(0.01)

            yield "data: [DONE]\n\n"

            # Adana bayanai a MongoDB
            chat_collection = get_chat_collection()
            if chat_collection is not None:
                new_user_msg = {"role": "user", "content": user_query or "[Fayil/Hoto]"}
                new_ai_msg = {"role": "assistant", "content": full_assistant_response}
                asyncio.create_task(
                    chat_collection.update_one(
                        {"_id": session_id},
                        {
                            "$set": {"user_email": user_email},
                            "$push": {"messages": {"$each": [new_user_msg, new_ai_msg]}}
                        },
                        upsert=True
                    )
                )

        except Exception as e:
            err_payload = json.dumps({"content": f"⚠️ Kuskure: {str(e)}"})
            yield f"data: {err_payload}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")