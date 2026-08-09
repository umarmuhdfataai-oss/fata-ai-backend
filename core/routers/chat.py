import asyncio
import base64
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
    """Hanya mai zurfi ta gano kowane irin umarnin zane ko kera hoto."""
    if not text:
        return False

    text_lower = text.lower().strip()

    explicit_matches = [
        "zananmin", "zanamin", "zannanmin", "hadamin", "haɗamin", "keramin", "kēramin",
        "yimin", "zanaminhoto", "zananminhoto", "generateimage", "drawimage", "key holder"
    ]
    if any(word in text_lower for word in explicit_matches):
        return True

    action_pattern = r'\b(zana|zāna|zannan|zanan|zanna|zayana|kera|kēra|hada|haɗa|yi|yimin|draw|drow|generate|create|make|paint|show)\b'
    image_pattern = r'\b(hoto|hoton|hotuna|image|images|photo|picture|pictures|keyholder|design)\b'

    has_action = bool(re.search(action_pattern, text_lower))
    has_image = bool(re.search(image_pattern, text_lower))

    return has_action and has_image


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

        # --- A. IDAN BUKATAR KERA HOTO CE (PRIMARY: IMAGEN 3, FALLBACK: FLUX) ---
        if is_image_request(user_query) and not file:
            try:
                yield f"data: {json.dumps({'content': '🎨 *Ina amfani da Injin Imagen 3 wajen ƙera maka hoto mai matukar kaifi...*\n\n'})}\n\n"
                await asyncio.sleep(0.1)

                def enhance_prompt():
                    try:
                        prompt_conversion = (
                            "Convert this Hausa request into an extremely detailed, high-resolution 8k photorealistic prompt for image generation. "
                            "Specify precise textures, lighting, camera angle, vivid colors, and crystal-clear subject details. "
                            "Output ONLY the optimized English prompt string without commentary or quotes.\n\n"
                            f"Hausa Request: {user_query}"
                        )
                        res = client.models.generate_content(
                            model="gemini-3.6-flash",
                            contents=prompt_conversion
                        )
                        return res.text.strip() if res.text else user_query
                    except Exception:
                        return f"Photorealistic 8k ultra-detailed image: {user_query}"

                english_prompt = await asyncio.to_thread(enhance_prompt)

                image_markdown = ""

                try:
                    def generate_imagen():
                        result = client.models.generate_images(
                            model="imagen-3.0-generate-002",
                            prompt=english_prompt,
                            config=types.GenerateImagesConfig(
                                number_of_images=1,
                                output_mime_type="image/jpeg",
                                aspect_ratio="1:1"
                            )
                        )
                        if result.generated_images:
                            img_bytes = result.generated_images[0].image.image_bytes
                            b64_img = base64.b64encode(img_bytes).decode("utf-8")
                            return f"data:image/jpeg;base64,{b64_img}"
                        return None

                    imagen_b64 = await asyncio.to_thread(generate_imagen)
                    if imagen_b64:
                        image_markdown = f"![{user_query}]({imagen_b64})\n\nGa hoton da Google Imagen 3 ta kera maka daidai da buƙatarka!"

                except Exception:
                    clean_prompt = urllib.parse.quote(english_prompt)
                    seed = random.randint(10000, 99999)
                    image_url = f"https://image.pollinations.ai/prompt/{clean_prompt}?width=1024&height=1024&model=flux&nologo=true&seed={seed}"
                    image_markdown = f"![{user_query}]({image_url})\n\nGa hoton da aka ƙera maka cikin ƙwarewa!"

                full_assistant_response = image_markdown
                yield f"data: {json.dumps({'content': image_markdown})}\n\n"
                yield "data: [DONE]\n\n"
                return

            except Exception as img_err:
                msg = "⚠️ An samu ɗan tsaiko wajen sarrafa hoton. Da fatan ka sake gwadawa."
                yield f"data: {json.dumps({'content': msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

        # --- B. IDAN HIRA CE TA DE-DA-DE (TEXT & SEARCH STABLE STREAM) ---
        target_model = model.strip() if model else "gemini-3.6-flash"

        contents = []
        if file:
            file_bytes = await file.read()
            mime_type = file.content_type or "image/jpeg"
            contents.append(types.Part.from_bytes(data=file_bytes, mime_type=mime_type))

        if user_query:
            contents.append(user_query)

        is_simple_greeting = user_query.lower() in ["slm", "salam", "salamu alaikum", "sannu", "hi", "hello"]

        def fetch_stream(use_google_search: bool):
            tools = [{"google_search": {}}] if use_google_search else None
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

        # Gwada kiran Gemini tare da Search idan ba gaisuwa ce kawai ba
        if not is_simple_greeting:
            try:
                response_stream = await asyncio.to_thread(fetch_stream, True)
            except Exception:
                # Idan Search din ya sami matsala ko Quota Error, tsarin zai sauya zuwa hira ta gaba daya ba tare da Search ba
                response_stream = None

        # Idan har yanzu ba a samu stream ba (ko gaisuwa ce), kirata ba tare da Search Tool ba
        if response_stream is None:
            try:
                response_stream = await asyncio.to_thread(fetch_stream, False)
            except Exception as e:
                err_text = str(e)
                if "429" in err_text or "RESOURCE_EXHAUSTED" in err_text:
                    msg = "⚠️ An samu matsalar iyakance amfani (Quota limit). Da fatan ka jira daƙiƙa kaɗan ka sake gwadawa ko ka sake sabunta API Key din ka."
                else:
                    msg = f"⚠️ Kuskure: {err_text}"
                yield f"data: {json.dumps({'content': msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

        # Aika chunks zuwa frontend
        try:
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