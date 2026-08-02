from fastapi import APIRouter, Depends, HTTPException, status
from core.database import get_chat_collection
from core.security import get_current_user

router = APIRouter(prefix="/history", tags=["Chat History Management"])

@router.get("/")
async def get_user_chat_history(current_user: dict = Depends(get_current_user)):
    user_email = current_user.get("sub")
    chat_collection = get_chat_collection()
    
    if chat_collection is None:
        raise HTTPException(status_code=500, detail="Database collection not initialized.")
        
    try:
        cursor = chat_collection.find({"user_email": user_email}, {"messages": 0}).sort("updated_at", -1)
        chats = []
        async for doc in cursor:
            chats.append({
                "session_id": doc.get("_id"),
                "title": doc.get("title", "Tattaunawa Sabuwa"),
                "chat_mode": doc.get("chat_mode", "chat"),
                "updated_at": doc.get("updated_at")
            })
        return {"status": "success", "chats": chats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{session_id}")
async def get_single_chat_session(session_id: str, current_user: dict = Depends(get_current_user)):
    chat_collection = get_chat_collection()
    if chat_collection is None:
        raise HTTPException(status_code=500, detail="Database collection not initialized.")
        
    chat = await chat_collection.find_one({"_id": session_id})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat session not found.")
        
    return {"status": "success", "session_id": chat.get("_id"), "messages": chat.get("messages", [])}