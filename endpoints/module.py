from datetime import datetime
from dateutil.relativedelta import relativedelta

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
from datetime import datetime

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


def get_filtered_borrowers_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    filtered_borrowers_collection = db["test_filtered_borrowers"]
    return filtered_borrowers_collection


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

MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
]

@router.post('/module', response_model=ModuleResponse, response_model_by_alias=False, response_description="Module added successfully", status_code=status.HTTP_201_CREATED)
async def create_module(filters: TransactionFilters, module_name: str = Body(...), project_id: str = Body(...), user: UserBaseSchema = Depends(get_current_user)):
    try:
        # Getting collection
        collection_filtered_borrowers = get_filtered_borrowers_collection()

        # Initializing query filter with current project_id to filter group_mvp documents
        query_filter = {"ProjectId": project_id}

        # Setting amount filter
        if filters.amount and filters.amount.value:
            if filters.amount.prefix == "less_than_equal_to":
                query_filter["LC_TotalLoanAmount"] = {
                    "$lte": filters.amount.value}

            if filters.amount.prefix == "greater_than_equal_to":
                query_filter["LC_TotalLoanAmount"] = {
                    "$gte": filters.amount.value}

        # Setting transaction count filter
        # if filters.transaction_count and filters.transaction_count.value:
        #     if filters.transaction_count.prefix == "less_than_equal_to":
        #         query_filter["LC_NumberOfTransactions"] = {
        #             "$lte": filters.transaction_count.value}
                
        #     if filters.transaction_count.prefix == "greater_than_equal_to":
        #         query_filter["LC_NumberOfTransactions"] = {
        #             "$gte": filters.transaction_count.value}
        
        # Setting transaction year filter
        if filters.transaction_year and filters.transaction_year.value:
            if filters.transaction_year.prefix == "less_than_equal_to":
                query_filter["LC_LatestTransactionDate"] = {
                    "$lte": filters.transaction_year.value}
                
            if filters.transaction_year.prefix == "greater_than_equal_to":
                query_filter["LC_LatestTransactionDate"] = {
                    "$gte": filters.transaction_year.value}

        # Setting property state/county filter
        if filters.states and len(filters.states) != 0:
            if (filters.county_codes and len(filters.county_codes) > 0):
                query_filter["$or"] = [{"FIPSCodeSet": {"$in": filters.county_codes}}]
            else:
                query_filter["$or"] = [{f"PropertyState.{state}": {
                    "$exists": True}} for state in filters.states]

        # Setting property state/county filter
        # if filters.states and len(filters.states) != 0:
        #     if (filters.county_codes and len(filters.county_codes) > 0):
        #         query_filter["$or"] = [{f"FIPSCode.{county_fips}": {
        #             "$exists": True}} for county_fips in filters.county_codes]
        #     else:
        #         query_filter["$or"] = [{f"PropertyState.{state}": {
        #             "$exists": True}} for state in filters.states]
        
        # Setting Borrower type filter
        # if filters.borrower_type and filters.borrower_type != "All Borrowers":
        #     borrower_type = 'I' if filters.borrower_type.lower() == "individual" else 'C'
        #     query_filter[f"LC_Type.{borrower_type}"] = {"$exists": True}

        filtered_documents_count = collection_filtered_borrowers.count_documents(query_filter)

        query_filter["module_id"] = None

        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": None,  # Replace with the field you want to group by
                "filtered_documents_untag_count": {"$sum": 1},
                "total_loan_count": {"$sum": "$LC_NumberOfLoans"},
                "total_loan_amount": {"$sum": "$LC_TotalLoanAmount"},
                "total_partial_loan_amount": {"$median": {
                    "input": "$LC_TotalLoanAmount",
                    "method": "approximate"
                }},
                "unique_DPID_keys": {"$addToSet": {
                    "$map": {
                        "input": {"$objectToArray": "$DPID"},
                        "as": "pair",
                        "in": "$$pair.k"
                    }
                }},
            }},
            {
                "$project": {
                    "_id": 0,
                    "filtered_documents_untag_count": 1,
                    "total_loan_count": 1,
                    "total_loan_amount": 1,
                    "total_properties": {"$size": "$unique_DPID_keys"},
                    "total_partial_loan_amount": 1,
                }
            }
        ]
        
        result = list(collection_filtered_borrowers.aggregate(pipeline=pipeline))

        if(len(result)==0):
            raise HTTPException(status_code=400, detail="No data found for the given filters")
        
        filtered_documents_untag_count, total_loan_count, total_loan_amount, total_partial_loan_amount, total_properties = result[0].values()
         
        already_taged_documents_count = filtered_documents_count - filtered_documents_untag_count
        
        project = get_project_collection().find_one({"_id": ObjectId(project_id)})

        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        
        project["_id"] = str(project["_id"])

        module = get_module_collection().find_one({"project_id":  project["_id"],"name": {"$regex": f"^{module_name.strip()}$", "$options": "i"}})

        if module:
            raise HTTPException(status_code=400, detail="Module already exists")
        
        date = datetime.now()
        all_months = []

        for _ in range(12):
            print(date.year, date.month)
            all_months.insert(0, {'year': date.year, 'month': date.month, 'count': 0})
            date -= relativedelta(months=1)  # subtract approximately one month



        # pipeline = [
        #     { "$match": query_filter},
        #     { "$unwind": "$LC_Transactions" },
        #     {
        #         "$addFields": {
        #             "LC_Transactions.OriginalDateOfContract": {
        #                 "$toDate": {
        #                     "$concat": [
        #                         {
        #                             "$substr": [
        #                                 { "$toString": "$LC_Transactions.OriginalDateOfContract" },
        #                                 0,
        #                                 4,
        #                             ],
        #                         },
        #                         "-",
        #                         {
        #                             "$substr": [
        #                                 { "$toString": "$LC_Transactions.OriginalDateOfContract" },
        #                                 4,
        #                                 2,
        #                             ],
        #                         },
        #                         "-",
        #                         {
        #                             "$substr": [
        #                                 { "$toString": "$LC_Transactions.OriginalDateOfContract" },
        #                                 6,
        #                                 2,
        #                             ],
        #                         },
        #                     ],
        #                 },
        #             },
        #         },
        #     },
        #     {
        #         "$match": {
        #             "LC_Transactions.OriginalDateOfContract": {
        #             "$gte": datetime(2022, 1, 1),
        #             "$lt": datetime(2024, 1, 1),
        #             },
        #         },
        #     },
        #     {
        #         "$group": {
        #             "_id": {
        #             "year": { "$year": "$LC_Transactions.OriginalDateOfContract" },
        #             "month": { "$month": "$LC_Transactions.OriginalDateOfContract" },
        #             },
        #             "count": { "$sum": 1 },
        #         },
        #     },
        #     {
        #         "$project": {
        #             "_id": 0,
        #             "year": "$_id.year",
        #             "month": "$_id.month",
        #             "count": 1,
        #         },
        #     }
        # ]


        pipeline = [
            {"$match": query_filter},
            {"$unwind": "$LC_Transactions"},
            {
                "$addFields": {
                    "LC_Transactions.OriginalDateOfContract": {
                        "$cond": {
                            "if": {
                                "$regexMatch": {
                                    # "input": "$LC_Transactions.OriginalDateOfContract",
                                    "input": {"$toString": "$LC_Transactions.OriginalDateOfContract"},
                                    "regex": "^[0-9]+$",  # Regex to check if the input is numeric
                                }
                            },
                            "then": {
                                "$dateFromString": {
                                    "dateString": {
                                        "$concat": [
                                            {
                                                "$substr": [
                                                    "$LC_Transactions.OriginalDateOfContract",
                                                    0,
                                                    4,
                                                ]
                                            },
                                            "-",
                                            {
                                                "$substr": [
                                                    "$LC_Transactions.OriginalDateOfContract",
                                                    4,
                                                    2,
                                                ]
                                            },
                                            "-",
                                            {
                                                "$substr": [
                                                    "$LC_Transactions.OriginalDateOfContract",
                                                    6,
                                                    2,
                                                ]
                                            },
                                        ]
                                    },
                                    "timezone": "UTC",
                                }
                            },
                            "else": None,  # Handle invalid date strings as needed
                        },
                    },
                },
            },
            {
                "$match": {
                    "LC_Transactions.OriginalDateOfContract": {
                        "$gte": datetime(2022, 1, 1),
                        "$lt": datetime(2024, 1, 1),
                    },
                },
            },
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$LC_Transactions.OriginalDateOfContract"},
                        "month": {"$month": "$LC_Transactions.OriginalDateOfContract"},
                    },
                    "count": {"$sum": 1},
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "count": 1,
                },
            },
        ]


        result = list(get_filtered_borrowers_collection().aggregate(pipeline=pipeline))

        for month in all_months:
            for item in result:
                if item["year"] == month["year"] and item["month"] == month["month"]:
                    month["count"] = item["count"]
                    break
            
            month["month"] = MONTH_NAMES[month["month"] - 1]
        
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
            "monthly_transactions_for_past_12_months": all_months,
            "total_loan_count": total_loan_count,
            "total_properties": total_properties,
            "total_loan_amount": total_loan_amount,
            "total_partial_loan_amount": total_partial_loan_amount,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }

        module_obj = ModuleSchema(**module_obj)

        response = get_module_collection().insert_one(module_obj.model_dump(by_alias=True, exclude=["id"]))
        
        collection_filtered_borrowers.update_many(
            query_filter, {"$set": {"module_id": response.inserted_id}})
        
        filtered_modules = collection_filtered_borrowers.find({"module_id": response.inserted_id}, {"_id": 0})

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


@router.get('/module/all', response_description="List of modules for particular project", response_model=ModuleResponse, response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def get_modules(
        project_id: str = Query(...),
        page_size: int = Query(10, ge=1),
        page: int = Query(1, ge=1),
        sort_by: str = Query(None)):
    try:
        collection_module = get_module_collection()

        project = get_project_collection().find_one({"_id": ObjectId(project_id)})

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

        return ModuleResponse(result=module, message="Module retrieved successfully",total=1)

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
