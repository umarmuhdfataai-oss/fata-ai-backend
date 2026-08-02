import json
import asyncio
import os
import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from groq import Groq
from core.security import get_current_user
from core.database import get_chat_collection

router = APIRouter(prefix="/chat", tags=["AI Chat Engine"])

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None

async def fetch_tavily_search(query: str) -> str:
    if not TAVILY_API_KEY:
        return ""
    
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "basic",
        "max_results": 2
    }
    
    try:
        async with httpx.AsyncClient(timeout=8.0) as httpx_client:
            response = await httpx_client.post(url, json=payload)
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                snippets = [res.get("content", "") for res in results if res.get("content")]
                if snippets:
                    return "\n".join(snippets)
    except Exception as e:
        print(f"⚠️ Tavily Search Error: {str(e)}")
        
    return ""

@router.post("/stream")
async def stream_chat(
    req: ChatRequest,
    current_user: dict = Depends(get_current_user)
):
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    if not client:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY is not configured.")

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"
    user_query = req.message.strip()

    # Binciko intanet idan akwai bukata
    web_search_context = await fetch_tavily_search(user_query)

    chat_collection = get_chat_collection()
    
    # Tsayayyen umarni da ke hana AI rudewa
    system_instruction = (
        "Sunanka Fata AI, babban mataimakin fasaha mai amfani da Groq LPU Engine. "
        "Amsa duk tambayoyi cikin harshen Hausa mai dadi, sauri, da cikakken bayani kamar Gemini. "
        "Yanzu muna cikin shekara ta 2026. "
        "Kayi kiyaye matuka wajen fitar da bayanan wasanni ko tarihi: kar ka karbi duk wani labarin karya ko jita-jita daga intanet. "
        "Ga ingantaccen tarihin gasar UEFA Champions League ta kakar 2025/2026 da aka buga a Budapest: "
        "Paris Saint-Germain (PSG) ce ta lashe kofin bayan sun doke Arsenal a bugun penariti (4-3) bayan sun tashi 1-1 a lokacin wasa da karin lokaci."
    )

    if web_search_context:
        system_instruction += f"\n\n[Bayanan Intanet]:\n{web_search_context}"

    messages = [{"role": "system", "content": system_instruction}]
    
    if chat_collection is not None:
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat and "messages" in existing_chat:
            for msg in existing_chat["messages"][-6:]:
                role = "user" if msg["role"] == "user" else "assistant"
                messages.append({"role": role, "content": msg["content"]})

    messages.append({"role": "user", "content": user_query})

    async def event_generator():
        full_assistant_response = ""
        try:
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
            err_payload = json.dumps({"content": f"⚠️ Kuskure: {str(e)}"})
            yield f"data: {err_payload}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")