import json
import asyncio
import os
import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from google import genai
from google.genai import types
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
    model: Optional[str] = "gemini-2.5-flash"

async def fetch_tavily_search(query: str) -> str:
    if not TAVILY_API_KEY:
        return ""
     
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "max_results": 4,
        "include_domains": ["uefa.com", "espn.com", "bbc.com", "goal.com", "en.wikipedia.org"]
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

    # Dauko tsohon tarihi daga MongoDB domin ba wa AI damar fahimtar jigon maganar
    chat_collection = get_chat_collection()
    previous_messages = []
    
    if chat_collection is not None:
        existing_session = await chat_collection.find_one({"_id": session_id})
        if existing_session and "messages" in existing_session:
            # Dauko sakonni 10 na karshe domin adana memory
            for msg in existing_session["messages"][-10:]:
                role = "user" if msg.get("role") == "user" else "model"
                previous_messages.append(
                    types.Content(
                        role=role,
                        parts=[types.Part.from_text(text=msg.get("content", ""))]
                    )
                )

    # Yin binciken Intanet ta Tavily idan tambayar tana bukatar sabon bayani na 2026
    web_search_context = await fetch_tavily_search(user_query)
      
    system_prompt = (
        "Sunanka Fata AI, babban mataimakin fasaha kuma ƙwararren mai bincike da aka gina a kan Google Gemini. "
        "Amsa duk tambayoyin masu amfani cikin harshen Hausa mai daɗi, inganci, da cikakken bayani kamar Gemini. "
        "Yanzu muna cikin shekara ta 2026. "
        "Ka riƙa amfani da tsari mai kyau (headings, bolding, lists) wajen gabatar da amsoshinka."
    )

    if web_search_context:
        system_prompt += f"\n\n[Ingantattun Sabbin Bayanai daga Intanet na 2026]: \n{web_search_context}"

    async def event_generator():
        full_assistant_response = ""
        try:
            # Tabbatar da amfani da sahihin model kamar gemini-2.5-flash ko gemini-2.5-pro
            target_model = "gemini-2.5-flash"
            if req.model and "pro" in req.model.lower():
                target_model = "gemini-2.5-pro"

            # Haɗa tsohon tarihi da sabon sako a matsayin contents
            contents = previous_messages + [
                types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=user_query)]
                )
            ]

            response = await asyncio.to_thread(
                client.models.generate_content_stream,
                model=target_model,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.7,
                )
            )

            for chunk in response:
                if chunk.text:
                    full_assistant_response += chunk.text
                    payload = json.dumps({"content": chunk.text})
                    yield f"data: {payload}\n\n"

            yield "data: [DONE]\n\n"

            # Ajiye sakon a MongoDB
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