from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr

from core.database import get_db
from core.security import hash_password, verify_password, create_access_token, decode_access_token

router = APIRouter(tags=["Auth Engine"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


class UserRegister(BaseModel):
    full_name: str
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


async def get_current_user(token: str = Depends(oauth2_scheme)):
    """Middleware to protect routes requiring authentication."""
    if not token:
        return None
    
    payload = decode_access_token(token)
    if not payload or "sub" not in payload:
        return None
        
    return payload["sub"]


@router.post("/auth/register")
async def register_user(user_data: UserRegister):
    db = get_db()
    users_collection = db["users"]

    # Duba ko email yana nan a baya
    existing_user = await users_collection.find_one({"email": user_data.email})
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Wannan Email din yana amfani riga."
        )

    hashed_pwd = hash_password(user_data.password)
    new_user = {
        "full_name": user_data.full_name,
        "email": user_data.email,
        "password": hashed_pwd,
        "created_at": status.HTTP_200_OK
    }

    await users_collection.insert_one(new_user)
    
    token = create_access_token({"sub": user_data.email})
    return {"message": "Rajiya ta kammala cikin nasara!", "access_token": token, "token_type": "bearer"}


@router.post("/auth/login", response_model=TokenResponse)
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    db = get_db()
    users_collection = db["users"]

    user = await users_collection.find_one({"email": form_data.username})
    if not user or not verify_password(form_data.password, user["password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email ko Password ba daidai bane."
        )

    token = create_access_token({"sub": user["email"]})
    return {"access_token": token, "token_type": "bearer"}