from datetime import datetime

from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body, Query, BackgroundTasks, UploadFile, File, status
import subprocess
from typing import Optional
from pymongo import MongoClient
from pymongo.collection import Collection
import asyncio
from schemas.module import ModuleResponse
from Oauth import get_current_user, create_access_token
from config.db import get_collection
from schemas import CreateUserSchema, UserBaseSchema
from utils import hash_password, verify_password, upload_file
import os
import traceback
from schemas.prospect import ProspectResponse

router = APIRouter(
    prefix="",
    tags=["Prospect"],
    responses={404: {"description": "Not found"}},
)

_ = load_dotenv(find_dotenv())
mongo_url = os.getenv("MONGO_URL")


def get_project_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    project_collection = db["project"]
    return project_collection


def get_module_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    module_collection = db["module"]
    return module_collection


def get_prospects_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    mvp_group_collection = db["prospects"]
    return mvp_group_collection


@router.get('/prospects/all', response_description="List of modules", response_model=ProspectResponse, response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def get_prospects(
        module_id: Optional[str] = Query(None),
        page_size: int = Query(10, ge=1),
        page: int = Query(1, ge=1),):
    try:
        collection_module = get_module_collection()
        module = collection_module.find_one({"_id": ObjectId(module_id)})
        if not module:
            raise HTTPException(status_code=404, detail="Module not found")
        
        collection_prospects = get_prospects_collection()
        prospects = list(collection_prospects.find({"module_id": ObjectId(module_id)}).limit(page_size).skip((page - 1) * page_size))
        total_prospects = collection_prospects.count_documents({"module_id": ObjectId(module_id)})

        return ProspectResponse(message="Prospects fetched successfully", total=total_prospects, result=prospects)


    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/prospect/timeline')
async def get_prospect_timeline(module_id: str):
    try:
        collection_project = get_project_collection()
        collection_module = get_module_collection()
        module_object_id = ObjectId(module_id)
        module_document = collection_module.find_one({"_id": module_object_id})

        if not module_document:
            raise HTTPException(status_code=404, detail="Module not found")

        module_created_at = module_document.get("created_at")
        module_created_by = module_document.get("user_mail")

        project_id = module_document.get("project_id")
        if not project_id:
            raise HTTPException(status_code=404, detail="Project ID not found in the module")

        project_document = collection_project.find_one({"project_id": int(project_id)})
        print(project_document)

        if not project_document:
            raise HTTPException(status_code=404, detail="Project not found")

        project_created_at = project_document.get("created_at")
        project_name = project_document.get("project_name")

        result_dict = {
            "project_name": project_name,
            "module_created_by": module_created_by,
            "module_created_at": module_created_at,
            "metadata": {
                "raw": project_created_at,
                "bronze": module_created_at
            }
        }

        return {"msg": "prospect timeline fetched successfully", "data": result_dict}

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

# @router.get('/prospect/timeline')
# async def get_prospect_timeline(module_id: str):
#     try:
#         collection_project = get_project_collection()
#         collection_module = get_module_collection()
#         module_object_id = ObjectId(module_id)
#
#         # Perform a $lookup to get project information using the project_id from the module
#         pipeline = [
#             {
#                 "$match": {"_id": module_object_id}
#             },
#             {
#                 "$lookup": {
#                     "from": "collection_project",
#                     "localField": "project_id",
#                     "foreignField": "project_id",
#                     "as": "project_info"
#                 }
#             },
#             {
#                 "$unwind": "$project_info"  # Unwind the result of the lookup
#             }
#         ]
#
#         result = list(collection_module.aggregate(pipeline))
#
#         if not result:
#             raise HTTPException(status_code=404, detail="Module not found")
#
#         # Extract information from the result
#         module_created_at = result[0].get("created_at")
#         module_created_by = result[0].get("user_mail")
#         project_created_at = result[0]["project_info"].get("created_at")
#         project_name = result[0]["project_info"].get("project_name")
#
#         # Create a dictionary with the required information
#         result_dict = {
#             "project_created_at": project_created_at,
#             "module_created_at": module_created_at,
#             "module_created_by": module_created_by,
#             "project_name": project_name,
#         }
#
#         return {"msg": "prospect timeline fetched successfully", "prospects": result_dict}
#
#     except HTTPException as http_exception:
#         raise http_exception
#
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

