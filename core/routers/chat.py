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

# Sunan model mai inganci a Groq
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"

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
        "yimin", "zanaminhoto", "zananminhoto", "generateimage", "drawimage", "draw"
    ]
    if any(word in text_lower for word in explicit_matches):
        return True

    action_pattern = r'\b(zana|zāna|zannan|zanan|zanna|zayana|kera|kēra|hada|haɗa|yi|yimin|draw|drow|generate|create|make|paint|show)\b'
    image_pattern = r'\b(hoto|hoton|hotuna|image|images|photo|picture|pictures|keyholder|design|art|illustration)\b'

    has_action = bool(re.search(action_pattern, text_lower))
    has_image = bool(re.search(image_pattern, text_lower))

    return has_action and has_image


@router.post("/chat/stream")
async def stream_chat(
    message: str = Form(""),
    session_id: Optional[str] = Form(None),
    model: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None)
):
    if not message and not file:
        raise HTTPException(status_code=400, detail="Muna buƙatar saƙo ko fayil.")

    user_email = "guest_user"
    session_id = session_id if session_id else "default_session"
    user_query = message.strip() if message else ""

    # ADVANCED WORLD-CLASS SYSTEM PROMPT
    system_prompt = (
        "CURRENT YEAR: 2026.\n\n"
        "YOU ARE FATA AI: The most intelligent, friendly, and highly capable AI assistant on Earth, built to provide world-class responses.\n\n"
        "1. INTELLIGENCE & ACCURACY: Respond with deep wisdom, precision, and flawless logical reasoning. Provide clear, structured, and helpful answers.\n"
        "2. PERFECT MULTILINGUAL NATIVE SPEAKER: You effortlessly understand and naturally speak every global language (Hausa, English, Arabic, French, Fulani, Yoruba, Igbo, Spanish, etc.). ALWAYS respond in the exact language the user used.\n"
        "3. CONVERSATIONAL ELEGANCE: Be extremely engaging, warm, polite, and helpful. Speak naturally like a brilliant human friend.\n"
        "4. NO THINKING PROCESS OUTPUT: Do NOT show any thinking steps, reasoning process, or chain of thought. Provide ONLY the final answer."
    )

    async def event_generator():
        full_assistant_response = ""
        client = get_groq_client()

        if not client:
            msg = "⚠️ API Key bata inganta ba. Tabbatar ka saka GROQ_API_KEY a Render environment variables."
            yield f"data: {json.dumps({'content': msg})}\n\n"
            yield "data: [DONE]\n\n"
            return

        target_model = model.strip() if model else DEFAULT_GROQ_MODEL

        # FLUX IMAGE GENERATION WITH PROMPT ENHANCEMENT
        if is_image_request(user_query) and not file:
            try:
                yield f"data: {json.dumps({'content': '🎨 *Ina amfani da kaifin Flux Engine wajen zana hoton da ya fi kowane hoto kyau...*\n\n'})}\n\n"
                await asyncio.sleep(0.1)

                enhancement_prompt = (
                    f"Transform this simple user image request into an ultra-detailed, highly vivid, 8K resolution, cinematic English prompt for Flux image generator. "
                    f"Include lighting, mood, camera style, and photorealistic details. Return ONLY the enhanced prompt string without explanations: '{user_query}'"
                )

                res = await client.chat.completions.create(
                    model=target_model,
                    messages=[{"role": "user", "content": enhancement_prompt}],
                    temperature=0.7
                )
                english_prompt = res.choices[0].message.content.strip() if res.choices else user_query

                encoded_prompt = urllib.parse.quote(english_prompt)
                seed = random.randint(1, 999999)
                
                image_url = f"https://image.pollinations.ai/prompt/{encoded_prompt}?model=flux&width=1024&height=1024&seed={seed}&nologo=true"
                image_markdown = f"![{user_query}]({image_url})\n\n✨ **Ga gwanintar hoton da na kera maka da duk wata kwarewa!**"
                
                yield f"data: {json.dumps({'content': image_markdown})}\n\n"
                yield "data: [DONE]\n\n"
                return

            except Exception as e:
                msg = f"⚠️ Kuskuren kera hoto: {str(e)}"
                yield f"data: {json.dumps({'content': msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

        # STREAMING WITH THINKING FILTER
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

            is_thinking = False
            thinking_buffer = ""

            async for chunk in response_stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    text_chunk = chunk.choices[0].delta.content

                    # Filta don cire <think>...</think> idan model din yana aiko da su
                    if "<think>" in text_chunk:
                        is_thinking = True
                        continue
                    if "</think>" in text_chunk:
                        is_thinking = False
                        continue

                    if not is_thinking:
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