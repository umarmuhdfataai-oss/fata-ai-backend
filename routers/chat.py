import os
import datetime
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from groq import Groq

from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/chat", tags=["AI Chat Engine"])

class ChatRequest(BaseModel):
    message: str
    session_id: str

async def save_conversation(session_id: str, user_email: str, message: str, full_response: str):
    """
    Adana tattaunawa a MongoDB tare da cikakken saƙo (history).
    """
    chat_collection = get_chat_collection()
    if chat_collection is None:
        print("🚨 DB not initialized for conversation saving.")
        return
    
    try:
        history = []
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat:
            history = existing_chat.get("messages", [])
        
        history.append({"role": "user", "content": message})
        history.append({"role": "assistant", "content": full_response})
        
        await chat_collection.update_one(
            {"_id": session_id},
            {
                "$set": {
                    "user_email": user_email,
                    "messages": history,
                    "chat_mode": "standard",
                    "title": "AI Chat Session",
                    "updated_at": datetime.datetime.now(datetime.timezone.utc)
                }
            },
            upsert=True
        )
    except Exception as e:
        print(f"🚨 Conversation save error: {str(e)}")

@router.post("/stream")
async def chat_stream(
    req: ChatRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    try:
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="GROQ_API_KEY environment variable is unconfigured."
            )

        client = Groq(api_key=api_key)
        user_email = current_user.get("sub", "guest_user")

        def generate_chunks():
            full_response = ""
            success = False

            try:
                # Sarrafa tattaunawa ta amfani da Groq da Llama-3.3-70b
                response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[
                        {
                            "role": "system",
                            "content": "Ni ne Fata AI, mataimaki mai kaifin kwakwalwa. Ina amsa tambayoyi cikin sauki da Hausa ko Turanci."
                        },
                        {
                            "role": "user",
                            "content": req.message
                        }
                    ],
                    stream=True
                )

                for chunk in response:
                    if chunk.choices and chunk.choices[0].delta.content:
                        text_chunk = chunk.choices[0].delta.content
                        full_response += text_chunk
                        yield text_chunk

                success = True

            except Exception as err:
                print(f"⚠️ Groq API Error: {str(err)}")
                err_msg = f"⚠️ An samu kuskure wajen sarrafa saƙo daga Groq API: {str(err)}"
                yield err_msg

            # Adana tattaunawa a MongoDB idan an samu amsa mai kyau
            if full_response and success:
                background_tasks.add_task(
                    save_conversation,
                    req.session_id,
                    user_email,
                    req.message,
                    full_response
                )

        return StreamingResponse(generate_chunks(), media_type="text/plain")

    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"🚨 Unexpected chat error: {str(e)}")
        raise HTTPException(status_code=500, detail="Chat engine internal failure.")