import os
import datetime
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "FATA_AI_ULTRA_SECURE_KEY_2026")
ALGORITHM = "HS256"
CryptContextRef = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v2/auth/login", auto_error=False)


def verify_password(plain_password, hashed_password):
    return CryptContextRef.verify(plain_password, hashed_password)


def get_password_hash(password):
    return CryptContextRef.hash(password)


def create_access_token(data: dict, expires_delta: datetime.timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.now(datetime.timezone.utc) + expires_delta
    else:
        expire = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=60)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)):
    if not token:
        return {"sub": "guest_user"}
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            return {"sub": "guest_user"}
        return payload
    except JWTError:
        return {"sub": "guest_user"}