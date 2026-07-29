import os
import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status, BackgroundTasks
from google import genai

from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/files", tags=["File Processing Engine"])

# Configure Gemini Client
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

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
            "content": "[File Uploaded & Processed via Gemini]",
            "file_uri": file_info.get("file_uri"),
            "file_name": file_info["file_name"],
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
        print(f"🚨 File DB Log Failure: {str(e)}")

@router.post("/upload")
async def upload_file_to_gemini(
    file: UploadFile = File(...),
    session_id: str = Form("default_session"),
    background_tasks: BackgroundTasks = None,
    current_user: dict = Depends(get_current_user)
):
    temp_file_path = f"temp_{file.filename}"
    try:
        if not client:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="GEMINI_API_KEY variable is unconfigured."
            )

        # Adana fayil a local temporary storage
        with open(temp_file_path, "wb") as f:
            f.write(await file.read())

        try:
            # Upload file zuwa Google Gemini Files API ta amfani da sabon client
            uploaded_file = client.files.upload(file=temp_file_path)
        except Exception as primary_err:
            print(f"⚠️ Primary Gemini upload error: {str(primary_err)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Gemini File upload failed: {str(primary_err)}"
            )
        finally:
            # Goge temporary file daga uwar garke (server)
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

        file_info = {
            "file_name": file.filename,
            "file_uri": getattr(uploaded_file, "uri", ""),
            "file_name_gemini": getattr(uploaded_file, "name", ""),
            "mime_type": file.content_type or "application/octet-stream"
        }

        user_email = current_user.get("sub", "guest_user")
        if background_tasks:
            background_tasks.add_task(log_file_upload, session_id, user_email, file_info)

        return {
            "status": "success",
            "engine": "Gemini File Processing",
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