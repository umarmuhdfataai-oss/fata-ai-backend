import os
from motor.motor_asyncio import AsyncIOMotorClient
import redis.asyncio as aioredis

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
REDIS_URI = os.environ.get("REDIS_URI", "redis://localhost:6379")

mongo_client = None
db = None
chat_collection = None
redis_client = None

async def connect_to_mongo():
    global mongo_client, db, chat_collection, redis_client
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client.get_database("fata_ai_v2_db")
    chat_collection = db.get_collection("conversations")
    
    try:
        redis_client = aioredis.from_url(REDIS_URI, decode_responses=True)
    except Exception as e:
        print(f"⚠️ Redis Connection Warning: {str(e)}")

async def close_mongo_connection():
    global mongo_client, redis_client
    if mongo_client:
        mongo_client.close()
    if redis_client:
        await redis_client.close()

def get_database():
    global db
    return db

def get_chat_collection():
    global chat_collection, db
    if chat_collection is not None:
        return chat_collection
    if db is not None:
        return db.get_collection("conversations")
    return None

def get_redis_client():
    return redis_client