from datetime import datetime

from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body
from pymongo import MongoClient
from pymongo.collection import Collection

from Oauth import get_current_user, create_access_token
from config.db import get_collection
from schemas import CreateUserSchema, UserBaseSchema
from utils import hash_password, verify_password
import os

router = APIRouter(
    prefix="/auth",
    tags=["Auth"],
    responses={404: {"description": "Not found"}},
)

_ = load_dotenv(find_dotenv())
mongo_url = os.getenv("MONGO_URL")
print(mongo_url)

def get_user_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    user_collection = db["user"]
    return user_collection


@router.post('/register')
async def create_user(user: CreateUserSchema):
    try:

        user_collection = get_user_collection()
        existing_user = user_collection.find_one({"email": user.email.lower()})
        if existing_user:
            raise HTTPException(status_code=409, detail='Account already exists')
        new_user = {
            "name": user.name,
            "phone": user.phone,
            "email": user.email.lower(),
            "password": hash_password(user.password),
            "role": user.role,
            "status": user.status,
            "is_authenticated": user.is_authenticated,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        result_user = user_collection.insert_one(new_user)
        return {"msg": "User registered successfully", "user_id": str(result_user.inserted_id)}

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/login')
def login(user_detail: dict = Body(..., description="requires email and password")):
    try:
        user_collection = get_user_collection()
        email = user_detail.get('email')
        password = user_detail.get('password')

        if not email or not password:
            raise HTTPException(status_code=401, detail='Need Both Email and Password')

        user = user_collection.find_one({"email": email.lower()})
        if not user:
            raise HTTPException(status_code=401, detail='Incorrect Email or Password')

        if not verify_password(password, user['password']):
            raise HTTPException(status_code=401, detail='Incorrect Password')

        # print(ObjectId(user['_id']))
        access_token = create_access_token(data={"user_id":  str(user['_id'])})
        return {"msg": "bearer token generated", "token": access_token}

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/me')
async def get_current_user(current_user: UserBaseSchema = Depends(get_current_user)):
    try:
        if current_user:
            current_user["_id"] = str(current_user["_id"])
            return {"user": current_user}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f'user not found : {str(e)}')
