import asyncio
import json
import os
import re
import base64
import urllib.parse
import random
from io import BytesIO
from typing import Optional
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import StreamingResponse
from gtts import gTTS
from groq import AsyncGroq
from duckduckgo_search import DDGS
import pypdf

from slowapi import Limiter
from slowapi.util import get_remote_address

from core.database import get_chat_collection
from core.interpreter import execute_python_code

router = APIRouter(tags=["Fata AI Engine"])
limiter = Limiter(key_func=get_remote_address)

DEFAULT_GROQ_MODEL = "qwen/qwen3.6-27b"
VISION_GROQ_MODEL = "llama-3.2-11b-vision-preview"
WHISPER_GROQ_MODEL = "whisper-large-v3"


def get_groq_client() -> Optional[AsyncGroq]:
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not api_key:
        return None
    return AsyncGroq(api_key=api_key)


def perform_global_search(query: str, max_results: int = 5) -> str:
    try:
        results = DDGS().text(query, max_results=max_results)
        if not results:
            return ""
        
        search_snippets = []
        for r in results:
            title = r.get("title", "")
            body = r.get("body", "")
            search_snippets.append(f"• {title}: {body}")
            
        return "\n".join(search_snippets)
    except Exception as e:
        print(f"Search Error: {e}")
        return ""


def extract_pdf_text(file_bytes: bytes) -> str:
    try:
        pdf_reader = pypdf.PdfReader(BytesIO(file_bytes))
        extracted_text = ""
        for page in pdf_reader.pages:
            text = page.extract_text()
            if text:
                extracted_text += text + "\n"
        return extracted_text.strip()
    except Exception as e:
        print(f"PDF Parsing Error: {e}")
        return ""


def is_image_request(text: str) -> bool:
    if not text:
        return False

    text_lower = text.lower().strip()
    explicit_matches = [
        "zananmin", "zanamin", "zannanmin", "hadamin", "haɗamin", "keramin", "kēramin",
        "yimin", "zanaminhoto", "zananminhoto", "generateimage", "drawimage", "draw", "generate image", "paint"
    ]
    if any(word in text_lower for word in explicit_matches):
        return True

    action_pattern = r'\b(zana|zāna|zannan|zanan|zanna|zayana|kera|kēra|hada|haɗa|yi|yimin|draw|drow|generate|create|make|paint|show)\b'
    image_pattern = r'\b(hoto|hoton|hotuna|image|images|photo|picture|pictures|keyholder|design|art|illustration)\b'

    return bool(re.search(action_pattern, text_lower)) and bool(re.search(image_pattern, text_lower))


@router.post("/chat/stream")
@limiter.limit("20/minute")
async def stream_chat(
    request: Request,
    message: str = Form(""),
    session_id: Optional[str] = Form(None),
    model: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    audio_file: Optional[UploadFile] = File(None)
):
    if not message and not file and not audio_file:
        raise HTTPException(status_code=400, detail="Muna buƙatar saƙo, fayil, ko sauti.")

    user_email = "guest_user"
    session_id = session_id if session_id else "default_session"
    user_query = message.strip() if message else ""

    async def event_generator():
        nonlocal user_query  # GYARA: An dawo da shi nan farkon aikin
        full_assistant_response = ""
        client = get_groq_client()

        if not client:
            msg = "⚠️ API Key bata inganta ba. Tabbatar ka saka GROQ_API_KEY a Render environment variables."
            yield f"data: {json.dumps({'content': msg})}\n\n"
            yield "data: [DONE]\n\n"
            return

        target_model = model.strip() if model else DEFAULT_GROQ_MODEL

        # 1. CODE INTERPRETER CHECK
        if user_query.startswith("```python") and user_query.endswith("```"):
            clean_code = user_query.replace("```python", "").replace("```", "").strip()
            yield f"data: {json.dumps({'content': '⚙️ *Fata AI Python Interpreter is executing your code...*\n\n'})}\n\n"
            execution_result = execute_python_code(clean_code)
            
            output_msg = f"```text\n{execution_result}\n```"
            yield f"data: {json.dumps({'content': output_msg})}\n\n"
            yield "data: [DONE]\n\n"
            return

        # 2. WHISPER VOICE TRANSCRIPTION
        if audio_file:
            try:
                yield f"data: {json.dumps({'content': '🎙️ *Fata AI Voice Engine is transcribing your audio...*\n\n'})}\n\n"
                audio_bytes = await audio_file.read()
                
                transcription = await client.audio.transcriptions.create(
                    file=(audio_file.filename, audio_bytes),
                    model=WHISPER_GROQ_MODEL,
                    prompt="Transcribe accurately."
                )
                
                transcribed_text = transcription.text.strip()
                if transcribed_text:
                    user_query = transcribed_text
                    yield f"data: {json.dumps({'content': f'🗣️ **Muryarka:** *\"{user_query}\"*\n\n---\n\n'})}\n\n"
            except Exception as voice_err:
                msg = f"⚠️ Voice error: {str(voice_err)}"
                yield f"data: {json.dumps({'content': msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

        # 3. FLUX IMAGE GENERATION
        if is_image_request(user_query) and not file:
            try:
                yield f"data: {json.dumps({'content': '🎨 *Fata AI Flux Engine is generating your image...*\n\n'})}\n\n"
                await asyncio.sleep(0.1)

                enhancement_prompt = (
                    f"Transform this user image request into an ultra-detailed English prompt for Flux image generator. "
                    f"Return ONLY the enhanced prompt string: '{user_query}'"
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
                image_markdown = f"![{user_query}]({image_url})\n\n✨ **Ga gwanintar hoton da Fata AI ya kera maka!**"
                
                yield f"data: {json.dumps({'content': image_markdown})}\n\n"
                yield "data: [DONE]\n\n"
                return

            except Exception as e:
                msg = f"⚠️ Image generation error: {str(e)}"
                yield f"data: {json.dumps({'content': msg})}\n\n"
                yield "data: [DONE]\n\n"
                return

        # 4. FILE PARSING (PDF OR VISION)
        user_message_content = []
        pdf_text_context = ""

        if file:
            file_bytes = await file.read()
            
            if file.filename.endswith(".pdf") or file.content_type == "application/pdf":
                extracted_pdf_text = extract_pdf_text(file_bytes)
                if extracted_pdf_text:
                    pdf_text_context = f"\n\nDOCUMENT CONTENT:\n{extracted_pdf_text[:12000]}"
                    yield f"data: {json.dumps({'content': '📄 *Fata AI has read your PDF document...*\n\n'})}\n\n"
            
            elif file.content_type.startswith("image/"):
                try:
                    base64_image = base64.b64encode(file_bytes).decode('utf-8')
                    image_data_url = f"data:{file.content_type};base64,{base64_image}"

                    user_message_content = [
                        {"type": "text", "text": user_query or "Bincika wannan hoton sannan ka fada min abinda kake gani a ciki."},
                        {"type": "image_url", "image_url": {"url": image_data_url}}
                    ]
                    target_model = VISION_GROQ_MODEL
                except Exception as img_err:
                    msg = f"⚠️ Image reading error: {str(img_err)}"
                    yield f"data: {json.dumps({'content': msg})}\n\n"
                    yield "data: [DONE]\n\n"
                    return

        if not user_message_content:
            user_message_content = user_query

        # 5. CHAT HISTORY (MONGODB MEMORY)
        history_messages = []
        try:
            chat_collection = get_chat_collection()
            if chat_collection is not None:
                chat_doc = await chat_collection.find_one({"_id": session_id})
                if chat_doc and "messages" in chat_doc:
                    recent_history = chat_doc["messages"][-6:]
                    for h in recent_history:
                        history_messages.append({"role": h["role"], "content": h["content"]})
        except Exception as history_err:
            print(f"Memory Fetch Error: {history_err}")

        # 6. REAL-TIME SEARCH
        search_context = ""
        if user_query and not file and len(user_query) > 2:
            try:
                search_query_res = await client.chat.completions.create(
                    model=target_model,
                    messages=[{
                        "role": "user", 
                        "content": f"Optimize query for search engine. Output ONLY query string: '{user_query}'"
                    }],
                    temperature=0.1
                )
                optimized_query = search_query_res.choices[0].message.content.strip() if search_query_res.choices else user_query

                search_results = await asyncio.to_thread(perform_global_search, optimized_query)
                if search_results:
                    search_context = f"\n\nREAL-TIME LIVE SEARCH DATA:\n{search_results}"
            except Exception as search_err:
                print(f"Search error: {search_err}")

        # SYSTEM PROMPT
        system_prompt = (
            "CURRENT YEAR: 2026.\n\n"
            "YOU ARE FATA AI: The supreme, ultra-intelligent, highly empathetic, and globally versatile AI system.\n\n"
            "GLOBAL CAPABILITIES & INSTRUCTIONS:\n"
            "1. UNIVERSAL NATIVE SPEAKER: Speak native Hausa, English, Arabic, French, and all languages accurately.\n"
            "2. CODE INTERPRETER & PDF VISION: Process code execution, audio, images, and full PDF files.\n"
            "3. REAL-TIME FACTUAL PRECISION: Rely on REAL-TIME LIVE SEARCH DATA for latest factual context.\n"
            "4. NO INTERNAL THINKING OUTPUT: Do NOT output <think> tags or reasoning steps."
            f"{pdf_text_context}"
            f"{search_context}"
        )

        messages = [{"role": "system", "content": system_prompt}]
        if history_messages:
            messages.extend(history_messages)
        if user_message_content:
            messages.append({"role": "user", "content": user_message_content})

        try:
            response_stream = await client.chat.completions.create(
                model=target_model,
                messages=messages,
                temperature=0.6,
                stream=True
            )

            is_thinking = False

            async for chunk in response_stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    text_chunk = chunk.choices[0].delta.content

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

            # Save History
            try:
                chat_collection = get_chat_collection()
                if chat_collection is not None:
                    new_user_msg = {"role": "user", "content": user_query or "[File Upload]"}
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
            msg = f"⚠️ Error sending message: {str(stream_err)}"
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