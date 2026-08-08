import asyncio
import json
import os
import re
import random
import urllib.parse
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


def is_image_request(text: str) -> bool:
    """Hanya mai inganci ta gano ko mai amfani yana buƙatar kera hoto ne."""
    if not text:
        return False

    text_lower = text.lower().strip()

    explicit_matches = [
        "zananmin", "zanamin", "hadamin", "haɗamin", "keramin", "kēramin",
        "yimin", "zanaminhoto", "zananminhoto", "generateimage", "drawimage"
    ]
    if any(word in text_lower for word in explicit_matches):
        return True

    has_image_word = bool(re.search(r'\b(hoto|hoton|hotuna|image|images|photo|picture|pictures)\b', text_lower))
    has_action_word = bool(re.search(r'\b(zana|zāna|kera|kēra|hada|haɗa|yi|draw|drow|generate|create|make)\b', text_lower))

    return has_image_word and has_action_word


# ==========================================
# 1. HANYAR HIRA DA KERA HOTO A WURI GUDA (UNIFIED STREAM)
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
        "ZAMANI DA SHEKARA: Yanzu muna shekarar 2026.\n\n"
        "UMARNI DA TSARIN FATA AI:\n"
        "1. KAI NE FATA AI: Injin basira mai amsawa cikin harshen Hausa mai inganci da zalla ilimi.\n"
        "2. Wasan karshe na UEFA Champions League na kakar 2025/2026 ya riga ya faru a tsakiyar shekarar 2026 a garin Budapest, kasar Hungary. Kungiyar Paris Saint-Germain (PSG) ce ta lashe kofin bayan ta doke Arsenal a bugun daga kai sai mai tsaye (penalties).\n"
        "3. Idan aka tambaye ka labarai ko sakamakon wasanni na 2026, amsa cikin amincewa da gaskiya ba tare da dogon korafi ba.\n"
        "4. Kar ka maimaita gaisuwa mara amfani; tsaya tsaye a kan amsar mai tambaya."
    )

    async def event_generator():
        full_assistant_response = ""

        # --- A. IDAN BUKATAR KERA HOTO CE (HIGH-QUALITY ENHANCED PROMPT) ---
        if is_image_request(user_query) and not file:
            try:
                yield f"data: {json.dumps({'content': '🎨 *Ina kera maka hoton mai inganci da haske, da fatan ka jira kaɗan...*\n\n'})}\n\n"
                await asyncio.sleep(0.1)

                # Step 1: Gemini za ta faɗaɗa umarnin zuwa cike da fito da mutum da bayyananniyar fuska/siffa
                def enhance_prompt():
                    prompt_conversion = (
                        "Convert this Hausa request into a highly detailed, clear, photorealistic 8k English prompt for image generation. "
                        "Ensure the subject/person is fully visible, well-lit with clear facial and body features, action-focused, "
                        "and bright daylight lighting. Output ONLY the English prompt string without quotes.\n\n"
                        f"Hausa Request: {user_query}"
                    )
                    res = client.models.generate_content(
                        model="gemini-3.6-flash",
                        contents=prompt_conversion
                    )
                    return res.text.strip() if res.text else user_query

                english_prompt = await asyncio.to_thread(enhance_prompt)

                # Step 2: Amfani da Injin Flux ta hanyar Pollinations don samun inganci sosai
                clean_prompt = urllib.parse.quote(english_prompt)
                seed = random.randint(10000, 99999)
                image_url = f"https://image.pollinations.ai/prompt/{clean_prompt}?width=1024&height=1024&model=flux&nologo=true&seed={seed}"
                
                image_markdown = f"![{user_query}]({image_url})\n\nGa hoton da ka buƙaci a kera maka!"
                
                full_assistant_response = image_markdown
                yield f"data: {json.dumps({'content': image_markdown})}\n\n"
                yield "data: [DONE]\n\n"
                return

            except Exception as img_err:
                msg = f"⚠️ Kuskuren Kera Hoto: {str(img_err)}"
                yield f"data: {json.dumps({'content': msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

        # --- B. IDAN HIRA CE TA DE-DA-DE (TEXT & SEARCH STREAM) ---
        target_model = model.strip() if model else "gemini-3.6-flash"

        contents = []
        if file:
            file_bytes = await file.read()
            mime_type = file.content_type or "image/jpeg"
            contents.append(types.Part.from_bytes(data=file_bytes, mime_type=mime_type))

        if user_query:
            contents.append(user_query)

        is_simple_greeting = user_query.lower() in ["slm", "salam", "salamu alaikum", "sannu", "hi", "hello"]
        use_search = not is_simple_greeting

        def fetch_stream(with_search: bool):
            tools = [{"google_search": {}}] if with_search else None
            config = types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.7,
                tools=tools
            )
            return client.models.generate_content_stream(
                model=target_model,
                contents=contents,
                config=config
            )

        response_stream = None
        if use_search:
            try:
                response_stream = await asyncio.to_thread(fetch_stream, True)
                first_chunk = next(iter(response_stream), None)
                if first_chunk and first_chunk.text:
                    full_assistant_response += first_chunk.text
                    yield f"data: {json.dumps({'content': first_chunk.text})}\n\n"
            except Exception:
                response_stream = await asyncio.to_thread(fetch_stream, False)
        else:
            response_stream = await asyncio.to_thread(fetch_stream, False)

        try:
            if response_stream:
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
            msg = f"⚠️ Kuskure: {str(stream_err)}"
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