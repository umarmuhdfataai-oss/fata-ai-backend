import base64
import datetime
import os
import random
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel

from google import genai
from google.genai import types
from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/image", tags=["AI Image Generation Engine (Gemini Imagen 3)"])

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

class ImageGenerationRequest(BaseModel):
    prompt: str
    session_id: Optional[str] = None

async def log_image_to_mongodb(
    session_id_str: str, history_list: list, user_email: str
):
    chat_collection = get_chat_collection()
    if chat_collection is None:
        return

    try:
        await chat_collection.update_one(
            {"_id": session_id_str},
            {
                "$set": {
                    "user_email": user_email,
                    "messages": history_list,
                    "chat_mode": "image_generation",
                    "title": "Gemini Imagen 3 Workspace",
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
        if not client:
            raise HTTPException(status_code=500, detail="GEMINI_API_KEY is not configured.")

        user_email = current_user.get("sub", "guest_user")
        session_id = (
            req.session_id
            if req.session_id
            else f"img_session_{random.randint(1000, 9999)}"
        )

        if not req.prompt or not req.prompt.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Kera hoto yana buƙatar rubuta bayani (prompt).",
            )

        # Amfani da Asalin Imagen 3 Model wajen kera hoto
        result = client.models.generate_images(
            model='imagen-3.0-generate-002',
            prompt=req.prompt.strip(),
            config=types.GenerateImagesConfig(
                number_of_images=1,
                output_mime_type="image/jpeg",
                aspect_ratio="1:1"
            )
        )

        image_bytes = result.generated_images[0].image.image_bytes
        base64_image = base64.b64encode(image_bytes).decode("utf-8")
        image_data_uri = f"data:image/jpeg;base64,{base64_image}"

        chat_collection = get_chat_collection()
        history = []

        if chat_collection is not None:
            existing_chat = await chat_collection.find_one({"_id": session_id})
            if existing_chat:
                history = existing_chat.get("messages", [])

        history.append({"role": "user", "content": f"Kera mini hoton: {req.prompt}"})
        history.append({
            "role": "assistant",
            "content": "[Generated Image Asset via Gemini Imagen 3]",
            "image_url": image_data_uri,
        })

        background_tasks.add_task(
            log_image_to_mongodb, session_id, history, user_email
        )

        return {
            "status": "success",
            "engine": "Google Gemini Imagen 3 Engine",
            "prompt": req.prompt,
            "mime_type": "image/jpeg",
            "image_data": image_data_uri,
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"🚨 Image Generation Error: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Kuskure wajen kera hoto: {str(e)}"
        )