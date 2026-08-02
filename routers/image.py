import datetime
import os
import random
from typing import Optional
import urllib.parse
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel

from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/image", tags=["AI Image Generation Engine"])

class ImageGenerationRequest(BaseModel):
    prompt: str
    session_id: Optional[str] = None

async def log_image_to_mongodb(
    session_id_str: str, history_list: list, user_email: str
):
    chat_collection = get_chat_collection()
    if chat_collection is None:
        print("🚨 Image DB Log Failure: Chat collection is not initialized.")
        return

    try:
        await chat_collection.update_one(
            {"_id": session_id_str},
            {
                "$set": {
                    "user_email": user_email,
                    "messages": history_list,
                    "chat_mode": "image_generation",
                    "title": "AI Image Workspace",
                    "updated_at": datetime.datetime.now(datetime.timezone.utc),
                }
            },
            upsert=True,
        )
    except Exception as e:
        print(f"🚨 Image DB Log Failure: {str(e)}")

@router.post("/generate")
async def generate_creative_image(
    req: ImageGenerationRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user),
):
    try:
        user_email = current_user.get("sub", "guest_user")
        session_id = (
            req.session_id
            if req.session_id
            else f"img_session_{random.randint(1000, 9999)}"
        )

        if not req.prompt or not req.prompt.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Prompt string cannot be empty.",
            )

        random_seed = random.randint(1, 999999)
        encoded_prompt = urllib.parse.quote(req.prompt.strip())
        full_image_url = f"https://image.pollinations.ai/prompt/{encoded_prompt}?width=1024&height=1024&seed={random_seed}&model=flux&nologo=true"

        chat_collection = get_chat_collection()
        history = []

        if chat_collection is not None:
            existing_chat = await chat_collection.find_one({"_id": session_id})
            if existing_chat:
                history = existing_chat.get("messages", [])

        history.append(
            {"role": "user", "content": f"Kera mini hoton: {req.prompt}"}
        )
        history.append({
            "role": "assistant",
            "content": "[Generated Image Asset UI Ready]",
            "image_url": full_image_url,
        })

        background_tasks.add_task(
            log_image_to_mongodb, session_id, history, user_email
        )

        return {
            "status": "success",
            "engine": "Fata Image Flux Engine",
            "prompt": req.prompt,
            "mime_type": "image/jpeg",
            "image_url": full_image_url,
            "image_data": full_image_url,
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"🚨 Unexpected image error: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Image engine internal failure."
        )