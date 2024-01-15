from datetime import datetime

from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body, Query, BackgroundTasks, UploadFile, File
import subprocess
from pymongo import MongoClient
from pymongo.collection import Collection
import asyncio

from Oauth import get_current_user, create_access_token
from config.db import get_collection
from data import load_json
from schemas import CreateUserSchema, UserBaseSchema
from schemas.project import CreateProject
from utils import hash_password, verify_password, upload_file
import os

router = APIRouter(
    prefix="",
    tags=[""],
    responses={404: {"description": "Not found"}},
)

_ = load_dotenv(find_dotenv())
mongo_url = os.getenv("MONGO_URL")


def get_sam_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    sam_collection = db["test_sam"]
    return sam_collection

def get_deed_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    deed_collection = db["test_deed"]
    return deed_collection

@router.post('/upload_sam')
async def create_project(background_tasks: BackgroundTasks, file: UploadFile = File(None)):
    try:

        collection_sam = get_sam_collection()
        result_sam = []
        companies = []
        if file:
            response = await upload_file(file)

            if response.get("status_code") == 200:
                if response.get("type") == "json":
                    companies = response.get('data', dict())

                elif response.get("type") == "csv":
                    companies = response.get('data', [])

                elif response.get("type") == "xlsx":
                    companies = response.get('data', [])

            if not companies:
                return {"msg": "No Companies Provided in request"}

            result_sam = collection_sam.insert_many(companies)
        
        return {"msg": "Success", "data":len(result_sam.inserted_ids)}

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    

@router.post('/upload_deed')
async def create_project(background_tasks: BackgroundTasks, file: UploadFile = File(None)):
    try:

        collection_deed = get_deed_collection()
        result_deed = []
        companies = []
        if file:
            response = await upload_file(file)

            if response.get("status_code") == 200:
                if response.get("type") == "json":
                    companies = response.get('data', dict())

                elif response.get("type") == "csv":
                    companies = response.get('data', [])

                elif response.get("type") == "xlsx":
                    companies = response.get('data', [])

            if not companies:
                return {"msg": "No Companies Provided in request"}

            result_deed = collection_deed.insert_many(companies)
        
        return {"msg": "Success", "data":len(result_deed.inserted_ids)}

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))