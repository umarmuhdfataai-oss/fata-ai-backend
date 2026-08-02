import json
import asyncio
import os
import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from google import genai
from core.security import get_current_user
from core.database import get_chat_collection

router = APIRouter(prefix="/chat", tags=["AI Chat Engine (Gemini)"])

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")

# Fara ainihin Google GenAI Client
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

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
        "search_depth": "advanced",
        "max_results": 3,
        "include_domains": ["uefa.com", "espn.com", "bbc.com", "goal.com"]
    }
     
    try:
        async with httpx.AsyncClient(timeout=10.0) as httpx_client:
            response = await httpx_client.post(url, json=payload)
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                snippets = []
                for res in results:
                    if res.get("content"):
                        snippets.append(f"[{res.get('date', 'N/A')}]: {res.get('content')}")
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
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY is not configured.")

    user_email = current_user.get("sub", "guest_user")
    session_id = req.session_id if req.session_id else "default_session"
    user_query = req.message.strip()

    web_search_context = await fetch_tavily_search(f"Final result of {user_query} in 2026 season")
    chat_collection = get_chat_collection()
     
    system_prompt = (
        "Sunanka Fata AI, babban mataimakin fasaha kuma ƙwararren mai bincike da aka gina a kan Google Gemini. "
        "Amsa duk tambayoyin masu amfani cikin harshen Hausa mai daɗi, inganci, da cikakken bayani kamar Gemini. "
        "Yanzu muna cikin shekara ta 2026. "
        "Ka riƙa amfani da tsari mai kyau (headings, bolding, lists) wajen gabatar da amsoshinka."
    )

    if web_search_context:
        system_prompt += f"\n\n[Ingantattun Sabbin Bayanai daga Intanet]: \n{web_search_context}"

    full_input_text = f"{system_prompt}\n\nUser Query: {user_query}"

    async def event_generator():
        full_assistant_response = ""
        try:
            interaction = await asyncio.to_thread(
                client.interactions.create,
                model="gemini-3.6-flash",
                input=full_input_text
            )

            response_text = interaction.output_text if interaction and interaction.output_text else "Babu amsa."
            full_assistant_response = response_text

            chunk_size = 20
            for i in range(0, len(response_text), chunk_size):
                text_chunk = response_text[i:i + chunk_size]
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
            err_payload = json.dumps({"content": f"⚠️ Kuskure daga Gemini Engine: {str(e)}"})
            yield f"data: {err_payload}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")