import json
import asyncio
import os
import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

# Amfani da Groq SDK
from groq import Groq

from core.security import get_current_user
from core.database import get_chat_collection

router = APIRouter(prefix="/chat", tags=["AI Chat Engine"])

# Configure Groq Client
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None

async def fetch_web_search(query: str) -> str:
    """
    Binciko intanet ta amfani da DuckDuckGo Search domin samun sabbin bayanai.
    """
    url = "https://html.duckduckgo.com/html/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as httpx_client:
            response = await httpx_client.post(url, data={"q": query}, headers=headers)
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, "html.parser")
                results = []
                for a in soup.find_all("a", class_="result__snippet", limit=3):
                    text = a.get_text(strip=True)
                    if text:
                        results.append(text)
                if results:
                    return " | ".join(results)
    except Exception as e:
        print(f"⚠️ Web search error: {str(e)}")
    return ""

@router.post("/stream")
async def stream_chat(
    req: ChatRequest,
    current_user: dict = Depends(get_current_user)
):
    if not req.message or not req.message.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Message cannot be empty."
        )

    if not client:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="GROQ_API_KEY is not configured on the server."
        )

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"

    user_query = req.message.strip()

    # Binciko yanar gizo idan tambayar tana bukatar sabon bayani
    search_context = await fetch_web_search(user_query)

    # MongoDB Chat History Retrieve
    chat_collection = get_chat_collection()
    
    system_instruction = (
        "Ni ne Fata AI, mataimakin fasaha mai amfani da Groq LPU Engine. "
        "Amsa tambayoyi cikin harshen Hausa ko Turanci a sauƙaƙe. "
        "Idan aka baka sakamakon binciken intanet (Web Search Results) a ƙasa, kayi amfani da su wajen bada ingantacciyar amsa ta yanzu."
    )

    if search_context:
        system_instruction += f"\n\n[Web Search Results / Sabbin Bayanai]: {search_context}"

    messages = [
        {
            "role": "system",
            "content": system_instruction
        }
    ]
    
    if chat_collection is not None:
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat and "messages" in existing_chat:
            for msg in existing_chat["messages"][-6:]:  # Keep last 6 context messages
                role = "user" if msg["role"] == "user" else "assistant"
                messages.append({"role": role, "content": msg["content"]})

    # Add current prompt
    messages.append({"role": "user", "content": user_query})

    async def event_generator():
        full_assistant_response = ""
        try:
            # Amfani da llama-3.3-70b-versatile don samun ingantattun amsoshi masu ƙarfi
            response_stream = await asyncio.to_thread(
                client.chat.completions.create,
                model="llama-3.3-70b-versatile",
                messages=messages,
                stream=True
            )

            for chunk in response_stream:
                if chunk.choices[0].delta.content:
                    text_chunk = chunk.choices[0].delta.content
                    full_assistant_response += text_chunk
                    
                    payload = json.dumps({"content": text_chunk})
                    yield f"data: {payload}\n\n"
                    await asyncio.sleep(0.01)

            yield "data: [DONE]\n\n"

            # Save chat to MongoDB
            if chat_collection is not None:
                new_user_msg = {"role": "user", "content": user_query}
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
            print(f"🚨 Streaming Error: {str(e)}")
            err_payload = json.dumps({"content": f"⚠️ Kuskure daga Groq Engine: {str(e)}"})
            yield f"data: {err_payload}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")