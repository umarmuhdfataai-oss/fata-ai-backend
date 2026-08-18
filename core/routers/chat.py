import asyncio
import json
import os
import re
import urllib.parse
import random
from io import BytesIO
from typing import Optional
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from gtts import gTTS
from groq import AsyncGroq

from core.database import get_chat_collection

router = APIRouter(tags=["Fata AI Engine"])

def get_groq_client() -> Optional[AsyncGroq]:
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not api_key:
        return None
    return AsyncGroq(api_key=api_key)


def is_image_request(text: str) -> bool:
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


@router.post("/chat/stream")
async def stream_chat(
    message: str = Form(""),
    session_id: Optional[str] = Form(None),
    model: Optional[str] = Form("llama-3.3-70b-versatile"),
    file: Optional[UploadFile] = File(None)
):
    if not message and not file:
        raise HTTPException(status_code=400, detail="Muna buƙatar saƙo ko fayil.")

    user_email = "guest_user"
    session_id = session_id if session_id else "default_session"
    user_query = message.strip() if message else ""

    system_prompt = (
        "CURRENT YEAR: 2026.\n\n"
        "YOU ARE FATA AI: An ultra-fast, highly accurate AI assistant powered by Groq and Meta Llama 3.3 architecture.\n"
        "1. MULTILINGUAL SUPPORT: You support all global languages (Hausa, English, Arabic, French, Spanish, etc.) natively. Always respond in the exact same language used by the user.\n"
        "2. ACCURACY & CONTEXT: Provide precise, direct, and intelligent answers.\n"
        "3. TONE: Be direct, smart, clean, and concise. Avoid unnecessary preambles or repeating greetings."
    )

    async def event_generator():
        full_assistant_response = ""
        client = get_groq_client()

        if not client:
            msg = "⚠️ API Key bata inganta ba. Tabbatar ka saka GROQ_API_KEY a Render environment variables."
            yield f"data: {json.dumps({'content': msg})}\n\n"
            yield "data: [DONE]\n\n"
            return

        # FLUX IMAGE GENERATION
        if is_image_request(user_query) and not file:
            try:
                yield f"data: {json.dumps({'content': '🎨 *Ina kera maka hoton ta amfani da Flux Engine...*\n\n'})}\n\n"
                await asyncio.sleep(0.1)

                res = await client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[
                        {"role": "user", "content": f"Translate and enhance this image description into a detailed English prompt for an AI image generator: '{user_query}'. Return ONLY the refined English prompt."}
                    ]
                )
                english_prompt = res.choices[0].message.content.strip() if res.choices else user_query

                encoded_prompt = urllib.parse.quote(english_prompt)
                seed = random.randint(1, 999999)
                
                image_url = f"https://image.pollinations.ai/prompt/{encoded_prompt}?model=flux&width=1024&height=1024&seed={seed}&nologo=true"
                image_markdown = f"![{user_query}]({image_url})\n\nGa hoton da na kera maka!"
                
                yield f"data: {json.dumps({'content': image_markdown})}\n\n"
                yield "data: [DONE]\n\n"
                return

            except Exception as e:
                msg = f"⚠️ Kuskuren kera hoto: {str(e)}"
                yield f"data: {json.dumps({'content': msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

        # CHAT STREAMING WITH GROQ
        target_model = model or "llama-3.3-70b-versatile"
        messages = [{"role": "system", "content": system_prompt}]
        if user_query:
            messages.append({"role": "user", "content": user_query})

        try:
            response_stream = await client.chat.completions.create(
                model=target_model,
                messages=messages,
                temperature=0.7,
                stream=True
            )

            async for chunk in response_stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    text_chunk = chunk.choices[0].delta.content
                    full_assistant_response += text_chunk
                    yield f"data: {json.dumps({'content': text_chunk})}\n\n"

            yield "data: [DONE]\n\n"

            # Database Update
            try:
                chat_collection = get_chat_collection()
                if chat_collection is not None:
                    new_user_msg = {"role": "user", "content": user_query or "[Fayil]"}
                    new_ai_msg = {"role": "assistant", "content": full_assistant_response}
                    
                    await chat_collection.update_one(
                        {"_id": session_id},
                        {
                            "$set": {"user_email": user_email},
                            "$push": {"messages": {"$each": [new_user_msg, new_ai_msg]}}
                        },
                        upsert=True
                    )
            except Exception:
                pass

        except Exception as stream_err:
            msg = f"⚠️ Tsaiko a saƙo: {str(stream_err)}"
            yield f"data: {json.dumps({'content': msg})}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/chat/text-to-speech", tags=["Fata AI Voice Engine"])
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