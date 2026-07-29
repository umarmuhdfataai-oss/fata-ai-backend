import os
import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status, BackgroundTasks
from groq import Groq

from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/files", tags=["File Processing Engine"])

async def log_file_upload(session_id: str, user_email: str, file_info: dict):
    """
    Adana bayanin file upload a MongoDB.
    """
    chat_collection = get_chat_collection()
    if chat_collection is None:
        print("🚨 File DB Log Failure: Chat collection is not initialized.")
        return
    
    try:
        history = []
        existing_chat = await chat_collection.find_one({"_id": session_id})
        if existing_chat:
            history = existing_chat.get("messages", [])
        
        history.append({"role": "user", "content": f"Uploaded file: {file_info['file_name']}"})
        history.append({
            "role": "system",
            "content": "[File Uploaded Successfully]",
            "file_id": file_info["file_id"],
            "mime_type": file_info["mime_type"]
        })
        
        await chat_collection.update_one(
            {"_id": session_id},
            {
                "$set": {
                    "user_email": user_email,
                    "messages": history,
                    "chat_mode": "file_upload",
                    "title": "AI File Workspace",
                    "updated_at": datetime.datetime.now(datetime.timezone.utc)
                }
            },
            upsert=True
        )
    except Exception as e:
        print(f"🚨 File DB Log Thread Failure: {str(e)}")

@router.post("/upload")
async def upload_file_to_groq(
    file: UploadFile = File(...),
    session_id: str = Form("default_session"),
    background_tasks: BackgroundTasks = None,
    current_user: dict = Depends(get_current_user)
):
    temp_file_path = f"temp_{file.filename}"
    try:
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="GROQ_API_KEY pipeline variable is unconfigured."
            )

        # Adana fayil a gida (temporary local storage)
        with open(temp_file_path, "wb") as f:
            f.write(await file.read())

        client = Groq(api_key=api_key)

        try:
            with open(temp_file_path, "rb") as file_to_upload:
                uploaded_file = client.files.create(
                    file=file_to_upload,
                    purpose="user_data"
                )
        except Exception as primary_err:
            print(f"⚠️ Primary Groq upload error: {str(primary_err)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"File upload failed: {str(primary_err)}"
            )

        # Goge temporary file daga uwar garke
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

        file_info = {
            "file_name": file.filename,
            "file_id": uploaded_file.id,
            "mime_type": file.content_type or "application/octet-stream"
        }

        user_email = current_user.get("sub", "guest_user")
        if background_tasks:
            background_tasks.add_task(log_file_upload, session_id, user_email, file_info)

        return {
            "status": "success",
            **file_info
        }

    except HTTPException as he:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        raise he
    except Exception as e:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        print(f"🚨 Unexpected file upload error: {str(e)}")
        raise HTTPException(status_code=500, detail="File engine internal failure.")