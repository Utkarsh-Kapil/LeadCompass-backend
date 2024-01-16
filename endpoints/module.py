from datetime import datetime

from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body, Query, BackgroundTasks, UploadFile, File, status
import subprocess
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from fastapi.encoders import jsonable_encoder
import asyncio
from typing import Optional

from Oauth import get_current_user, create_access_token
from config.db import get_collection
from schemas import CreateUserSchema, UserBaseSchema
from schemas.module import TransactionFilters, ModuleSchema, ModuleResponse
from utils import hash_password, verify_password, upload_file
import os
import traceback

router = APIRouter(
    prefix="",
    tags=["Module"],
    responses={404: {"description": "Not found"}},
)

_ = load_dotenv(find_dotenv())
mongo_url = os.getenv("MONGO_URL")


def get_module_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    module_collection = db["module"]
    return module_collection


def get_group_mvp_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    mvp_group_collection = db["group_mvp"]
    return mvp_group_collection


def get_project_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    project_collection = db["project"]
    return project_collection

def get_prospects_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    prospects_collection = db["prospects"]
    return prospects_collection


@router.post('/module', response_model=ModuleResponse, response_model_by_alias=False, response_description="Module added successfully", status_code=status.HTTP_201_CREATED)
async def create_module(filters: TransactionFilters, module_name: str = Body(...), project_id: int = Body(...), user: UserBaseSchema = Depends(get_current_user)):
    try:
        print(project_id)
        print(user)

        query_filter = {"ProjectId": project_id}

        if filters.amount and filters.amount.value:
            if filters.amount.prefix == "less_than_equal_to":
                query_filter["RcCalTotalLoanAmount"] = {
                    "$lte": filters.amount.value}

            if filters.amount.prefix == "greater_than_equal_to":
                query_filter["RcCalTotalLoanAmount"] = {
                    "$gte": filters.amount.value}

        if filters.transaction_count and filters.transaction_count.value:
            if filters.transaction_count.prefix == "less_than_equal_to":
                query_filter["RcCalNumberOfTransactions"] = {
                    "$lte": filters.transaction_count.value}
                
            if filters.transaction_count.prefix == "greater_than_equal_to":
                query_filter["RcCalNumberOfTransactions"] = {
                    "$gte": filters.transaction_count.value}
        
        if filters.transaction_year and filters.transaction_year.value:
            if filters.transaction_year.prefix == "less_than_equal_to":
                query_filter["RcCalLatestTransactionDate"] = {
                    "$lte": filters.transaction_year.value}
                
            if filters.transaction_year.prefix == "greater_than_equal_to":
                query_filter["RcCalLatestTransactionDate"] = {
                    "$gte": filters.transaction_year.value}

        # if filters.duration and filters.duration.value: 

            # if filters.duration.value == 'All Durations':
            #     query_filter["RcCalLatestTransactionDate"] = {}

            # if filters.transaction_count.value == 'Less than 6 months':
            #     query_filter["RcCalLatestTransactionDate"] = {"$gt": 1}

            # if filters.transaction_count.value == 'Less than 1 year':
            #     query_filter["RcCalLatestTransactionDate"] = {"$lt": 10}

            # if filters.transaction_count.value == 'Greater than 1 year':
            #     query_filter["RcCalLatestTransactionDate"] = {"$gt": 10}

        if filters.states and len(filters.states) != 0:
            if (filters.county_codes and len(filters.county_codes) > 0):
                query_filter["$or"] = [{f"FIPSCode.{county_fips}": {
                    "$exists": True}} for county_fips in filters.county_codes]
            else:
                query_filter["$or"] = [{f"PropertyState.{state}": {
                    "$exists": True}} for state in filters.states]

        if filters.borrower_type and filters.borrower_type != "All Borrowers":
            borrower_type = 'I' if filters.borrower_type.lower() == "individual" else 'C'
            query_filter[f"RcCalType.{borrower_type}"] = {"$exists": True}

        collection_mvp_grp = get_group_mvp_collection()

        filtered_documents_count = collection_mvp_grp.count_documents(query_filter)

        query_filter["module_id"] = None

        filtered_documents_untag_count = collection_mvp_grp.count_documents(query_filter)
        
        if(filtered_documents_untag_count == 0):
            raise HTTPException(status_code=400, detail="No data found for the given filters")
        
        already_taged_documents_count = filtered_documents_count - filtered_documents_untag_count
        
        project = get_project_collection().find_one({"project_id": project_id})

        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        
        project["_id"] = str(project["_id"])

        module = get_module_collection().find_one({"project_id":  project["_id"],"name": {"$regex": f"^{module_name.strip()}$", "$options": "i"}})

        if module:
            raise HTTPException(status_code=400, detail="Module already exists")

        module_obj = {
            "name": module_name,
            "status": "bronze",
            "project_id": project["_id"],
            "project": project,
            "user_id": user["_id"],
            "user_email": user["email"],
            "filters": jsonable_encoder(filters),
            "filtered_documents_count": filtered_documents_count,
            "filtered_documents_untag_count": filtered_documents_untag_count,
            "already_taged_documents_count": already_taged_documents_count,
            "timeline": {
                "raw": {"created_at": project["created_at"]},
                "bronze": {"created_at": datetime.now()},
            },
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }

        module_obj = ModuleSchema(**module_obj)

        response = get_module_collection().insert_one(module_obj.model_dump(by_alias=True, exclude=["id"]))
        
        collection_mvp_grp.update_many(
            query_filter, {"$set": {"module_id": response.inserted_id}})
        
        filtered_modules = collection_mvp_grp.find({"module_id": response.inserted_id}, {"_id": 0})

        # Insert filtered modules into prospects collection
        get_prospects_collection().insert_many(list(filtered_modules))
       
        new_module = get_module_collection().find_one({"_id": response.inserted_id})

        return ModuleResponse(result=new_module, message="Module added successfully")

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/module/all', response_description="List of modules", response_model=ModuleResponse, response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def get_modules(
        project_id: Optional[int] = Query(None),
        page_size: int = Query(10, ge=1),
        page: int = Query(1, ge=1),
        sort_by: str = Query(None)):
    try:
        collection_module = get_module_collection()

        project = get_project_collection().find_one({"project_id": project_id})

        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        
        sort = -1 if sort_by == "Last entry" else 1

        filter_query = {"project_id": str(project["_id"])}

        modules = list(collection_module.find(filter_query, {"project": 0, "filters": 0, "timeline": 0}).sort("created_at", sort).limit(page_size).skip((page - 1) * page_size))

        return ModuleResponse(result=modules, total=collection_module.count_documents(filter_query), message="Modules retrieved successfully")

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/module/{id}', response_description="Single module", response_model=ModuleResponse, response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def get_modules(id: str):
    try:
        collection_module = get_module_collection()

        module = collection_module.find_one({"_id": ObjectId(id)})

        if not module:
            raise HTTPException(status_code=404, detail="Module not found")

        return ModuleResponse(result=module, message="Module retrieved successfully")

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
