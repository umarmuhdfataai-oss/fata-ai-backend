import asyncio
import base64
import json
import os
from io import BytesIO
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from gtts import gTTS

from google import genai
from google.genai import types

from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/chat", tags=["Fata AI Core Engine"])

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


# ==========================================
# 1. HANYAR HIRA DA BINCIKE (CHAT STREAMING)
# ==========================================
@router.post("/stream")
async def stream_chat(
    message: str = Form(""),
    session_id: Optional[str] = Form(None),
    model: Optional[str] = Form("gemini-3.6-flash"),
    file: Optional[UploadFile] = File(None),
    current_user: dict = Depends(get_current_user)
):
    if not message and not file:
        raise HTTPException(status_code=400, detail="Muna buƙatar saƙo ko fayil.")

    if not client:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY ba a tsara shi ba.")

    user_email = current_user.get("sub", "guest_user")
    session_id = session_id if session_id else "default_session"
    user_query = message.strip() if message else ""

    system_prompt = (
        "ZAMANI DA SHEKARA: Yanzu muna shekarar 2026 ne.\n\n"
        "UMARNI DA TSARIN FATA AI:\n"
        "1. KAI NE FATA AI: Manhaja mai karfin basira, ilimi, da fasaha matuka.\n"
        "2. Yi amfani da Google Search a duk lokacin da aka tambaye ka sababbin bayanai ko abubuwan da ke faruwa a yanzu.\n"
        "3. Amsa tambayoyi daki-daki cikin gamsarwa, Hausa mai inganci, da zalla ilimi.\n"
        "4. Kar ka maimaita gaisuwa mara amfani; tsaya tsaye a kan amsar mai tambaya."
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

            config = types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.7,
                tools=[{"google_search": {}}]
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

            # Adana a MongoDB
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


# ==========================================
# 2. HANYAR KERA HOTO (IMAGEN 3)
# ==========================================
@router.post("/generate-image", tags=["Fata AI Image Generation"])
async def generate_image(
    prompt: str = Form(...),
    aspect_ratio: Optional[str] = Form("1:1"),
    current_user: dict = Depends(get_current_user)
):
    if not prompt:
        raise HTTPException(status_code=400, detail="Babu bayanin hoto (prompt).")

    if not client:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY ba a tsara shi ba.")

    try:
        def create_image():
            return client.models.generate_images(
                model='imagen-3.0-generate-002',
                prompt=prompt,
                config=types.GenerateImagesConfig(
                    number_of_images=1,
                    output_mime_type="image/jpeg",
                    aspect_ratio=aspect_ratio or "1:1"
                )
            )

        result = await asyncio.to_thread(create_image)

        if result and result.generated_images:
            image_bytes = result.generated_images[0].image.image_bytes
            base64_image = base64.b64encode(image_bytes).decode('utf-8')
            
            return {
                "status": "Success",
                "prompt": prompt,
                "image_data": f"data:image/jpeg;base64,{base64_image}"
            }

        raise HTTPException(status_code=500, detail="An kasa kera hoton.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kuskuren Kera Hoto: {str(e)}")


# ==========================================
# 3. SABUWAR HANYAR SAUTI DA MURYA (VOICE TTS)
# ==========================================
@router.post("/text-to-speech", tags=["Fata AI Voice Engine"])
async def text_to_speech(
    text: str = Form(...),
    lang: Optional[str] = Form("ha")
):
    """
    Kewaye rubutun amsar Fata AI zuwa fayil din sauti na MP3
    """
    if not text:
        raise HTTPException(status_code=400, detail="Muna bukatar rubutun da za a maida sauti.")

    try:
        def generate_audio():
            tts = gTTS(text=text, lang=lang or "ha", slow=False)
            fp = BytesIO()
            tts.write_to_fp(fp)
            fp.seek(0)
            return fp

        audio_fp = await asyncio.to_thread(generate_audio)
        return StreamingResponse(audio_fp, media_type="audio/mpeg")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kuskuren Maida Sauti: {str(e)}")