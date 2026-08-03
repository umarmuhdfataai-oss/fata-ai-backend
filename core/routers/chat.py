import asyncio
import json
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse

from google import genai

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
        "UMARNI MAI MUHIMMANCI:\n"
        "1. Ka amsa tambayar da aka yi maka KAI TSAYE.\n"
        "2. KAR KA MAIMAITA gaisuwa ko cewa 'Ni ne Fata AI...' ko gabatar da kanka lokacin amsa tambaya.\n"
        "3. Yi amfani da Google Search wajen neman sabbin bayanai na shekarar 2026.\n"
        "4. Amsa cikin harshen Hausa mai inganci."
    )

    async def event_generator():
        full_assistant_response = ""
        try:
            target_model = "gemini-3.6-flash"
            if model and "pro" in model.lower():
                target_model = "gemini-3.6-pro"

            # Amfani da run_in_executor don kiran asalin aikin (blocking) kuma ka tattara iterator din
            loop = asyncio.get_event_loop()
            
            def get_stream():
                return client.interactions.create(
                    model=target_model,
                    input=user_query,
                    system_instruction=system_prompt,
                    tools=[{"type": "google_search"}],
                    stream=True
                )
                
            stream = await loop.run_in_executor(None, get_stream)

            # Iterating ta hanyar to_thread don gudun toshe loop
            for event in stream:
                text_chunk = None
                
                if hasattr(event, "delta") and event.delta:
                    if isinstance(event.delta, dict) and "text" in event.delta:
                        text_chunk = event.delta["text"]
                    elif hasattr(event.delta, "text") and event.delta.text:
                        text_chunk = event.delta.text
                elif hasattr(event, "output_text") and event.output_text:
                    text_chunk = event.output_text

                if text_chunk:
                    full_assistant_response += text_chunk
                    yield f"data: {json.dumps({'content': text_chunk})}\n\n"
                    # Muhimmanci: bada lokaci don tura sakon
                    await asyncio.sleep(0)

            yield "data: [DONE]\n\n"

            # Adana bayanai (Fire and Forget)
            chat_collection = get_chat_collection()
            if chat_collection is not None:
                new_user_msg = {"role": "user", "content": user_query or "[Hoto/Fayil]"}
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