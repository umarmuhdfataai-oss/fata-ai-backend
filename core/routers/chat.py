import asyncio
import json
import os
import re
import base64
from io import BytesIO
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from gtts import gTTS

from google import genai
from google.genai import types

from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/chat", tags=["Fata AI Pure Gemini Engine"])

# --- OFFICIAL GEMINI CLIENT SETUP ---
def get_gemini_client():
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        return None
    return genai.Client(api_key=api_key)


def is_image_request(text: str) -> bool:
    """Gano idan mai amfani yana buƙatar kera hoto."""
    if not text:
        return False

    text_lower = text.lower().strip()

    explicit_matches = [
        "zananmin", "zanamin", "zannanmin", "hadamin", "haɗamin", "keramin", "kēramin",
        "yimin", "zanaminhoto", "zananminhoto", "generateimage", "drawimage"
    ]
    if any(word in text_lower for word in explicit_matches):
        return True

    action_pattern = r'\b(zana|zāna|zannan|zanan|zanna|zayana|kera|kēra|hada|haɗa|yi|yimin|draw|drow|generate|create|make|paint|show)\b'
    image_pattern = r'\b(hoto|hoton|hotuna|image|images|photo|picture|pictures|keyholder|design)\b'

    has_action = bool(re.search(action_pattern, text_lower))
    has_image = bool(re.search(image_pattern, text_lower))

    return has_action and has_image


# ==========================================
# 1. PURE GEMINI CHAT & NATIVE IMAGEN 3
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

    user_email = current_user.get("sub", "guest_user")
    session_id = session_id if session_id else "default_session"
    user_query = message.strip() if message else ""

    # SYSTEM PROMPT FOR UNIVERSAL MULTILINGUAL & CURRENT CONTEXT
    system_prompt = (
        "CURRENT YEAR: 2026.\n\n"
        "YOU ARE FATA AI: An advanced AI collaborator powered strictly by Google Gemini technology.\n"
        "1. MULTILINGUAL SUPPORT: You support all global languages (Hausa, English, Arabic, French, Spanish, etc.) natively. Always respond in the exact same language used by the user.\n"
        "2. ACCURACY & CONTEXT: The 2025/2026 UEFA Champions League final took place in mid-2026 in Budapest, Hungary, where Paris Saint-Germain (PSG) defeated Arsenal on penalties.\n"
        "3. TONE: Be direct, smart, highly accurate, and concise. Avoid unnecessary preambles or repeating greetings."
    )

    async def event_generator():
        full_assistant_response = ""
        client = get_gemini_client()

        if not client:
            msg = "⚠️ API Key bata inganta ba. Tabbatar ka saka ainihin API Key daga Google AI Studio."
            yield f"data: {json.dumps({'content': msg})}\n\n"
            yield "data: [DONE]\n\n"
            return

        # --- A. NATIVE GOOGLE IMAGEN 3 GENERATION ---
        if is_image_request(user_query) and not file:
            try:
                yield f"data: {json.dumps({'content': '🎨 *Ina kera maka hoton ta amfani da Google Imagen 3...*\n\n'})}\n\n"
                await asyncio.sleep(0.1)

                def generate_imagen():
                    return client.models.generate_images(
                        model='imagen-3.0-generate-002',
                        prompt=user_query,
                        config=types.GenerateImagesConfig(
                            number_of_images=1,
                            output_mime_type="image/jpeg",
                            aspect_ratio="1:1"
                        )
                    )

                result = await asyncio.to_thread(generate_imagen)

                if result.generated_images:
                    for generated_image in result.generated_images:
                        b64_img = base64.b64encode(generated_image.image.image_bytes).decode('utf-8')
                        image_markdown = f"![{user_query}](data:image/jpeg;base64,{b64_img})\n\nGa hoton da na kera maka da Google Imagen 3 Engine!"
                        
                        full_assistant_response = image_markdown
                        yield f"data: {json.dumps({'content': image_markdown})}\n\n"
                        yield "data: [DONE]\n\n"
                        return

            except Exception as e:
                msg = f"⚠️ Kuskuren Imagen 3: {str(e)}\n*Tabbatar ka haɗa Billing Account dinka a Google AI Studio.*"
                yield f"data: {json.dumps({'content': msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

        # --- B. MULTI-LINGUAL CHAT & GOOGLE SEARCH GROUNDING ---
        target_model = model.strip() if model else "gemini-3.6-flash"

        contents = []
        if file:
            file_bytes = await file.read()
            mime_type = file.content_type or "image/jpeg"
            contents.append(types.Part.from_bytes(data=file_bytes, mime_type=mime_type))

        if user_query:
            contents.append(user_query)

        try:
            def fetch_gemini_stream():
                config = types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.7,
                    tools=[{"google_search": {}}]  # Full native search tool
                )
                return client.models.generate_content_stream(
                    model=target_model,
                    contents=contents,
                    config=config
                )

            response_stream = await asyncio.to_thread(fetch_gemini_stream)

            for chunk in response_stream:
                if chunk.text:
                    full_assistant_response += chunk.text
                    yield f"data: {json.dumps({'content': chunk.text})}\n\n"
                    await asyncio.sleep(0.01)

            yield "data: [DONE]\n\n"

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

        except Exception as stream_err:
            msg = f"⚠️ Tsaiko a saƙo: {str(stream_err)}"
            yield f"data: {json.dumps({'content': msg})}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ==========================================
# 2. HANYAR SAUTI DA MURYA (VOICE TTS)
# ==========================================
@router.post("/text-to-speech", tags=["Fata AI Voice Engine"])
async def text_to_speech(
    text: str = Form(...),
    lang: Optional[str] = Form("ha")
):
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