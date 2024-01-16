from datetime import datetime
from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body, Query, BackgroundTasks, UploadFile, File, status
import subprocess
from pymongo import MongoClient
from pymongo.collection import Collection
import asyncio

from Oauth import get_current_user, create_access_token
from config.db import get_collection
from data import load_json
from schemas import CreateUserSchema, UserBaseSchema
from schemas.project import ProjectSchema,ProjectResponse
from utils import hash_password, verify_password, upload_file
import os
import zipfile
import pandas as pd
import io
import json

router = APIRouter(
    prefix="",
    tags=["Project"],
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


def get_project_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    project_collection = db["project"]
    return project_collection

def run_scripts(id:str):

    project_id = id
    cur_dir = os.getcwd()
    script_directory = os.path.join(cur_dir, "scripts")

    script_filename = "master_script.py"  
    script_path = f"{script_directory}/{script_filename}"
    subprocess.run(["python", script_path, "--project_id", project_id])


@router.post('/project', response_model=ProjectResponse, response_model_by_alias=False, response_description="Project added successfully", status_code=status.HTTP_201_CREATED)
async def create_project(background_tasks: BackgroundTasks, file: UploadFile = File(None),
                         user: UserBaseSchema = Depends(get_current_user), project: ProjectSchema = None):
    try:

        collection_sam = get_sam_collection()
        collection_deed = get_deed_collection()
        result_sam = []
        companies = []
        # if file:
        #     response = await upload_file(file)

        #     if response.get("status_code") == 200:
        #         if response.get("type") == "json":
        #             companies = response.get('data', dict())

        #         elif response.get("type") == "csv":
        #             companies = response.get('data', [])

        #         elif response.get("type") == "xlsx":
        #             companies = response.get('data', [])

        #     if not companies:
        #         return {"msg": "No Companies Provided in request"}

        #     result_sam = collection_sam.insert_many(companies)

        # else:
        #     companies = load_json.company_data
        #     result_sam = collection_sam.insert_many(companies)

        # inserted_ids = result_sam.inserted_ids

        collection_project = get_project_collection()
        new_project = {
            "id": ObjectId(),
            "user_email": user.get('email'),
            "total_mortgage_transaction": len(companies),
            "created_at": datetime.now(),
            "status": "processing"
        }

        source = "blackknight"
        if file:
            new_project["source"] = file.filename.split(".")[-1].lower()
            new_project["project_name"] = f"{file.filename}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        else:
            new_project["source"] = source
            new_project["project_name"] = f"blackknight_{datetime.now().strftime('%Y%m%d%H%M%S')}"


        new_project = ProjectSchema(**new_project)

        result_project = collection_project.insert_one(new_project.model_dump(by_alias=True, exclude=["id"]))
        inserted_id = result_project.inserted_id

        # collection_sam.update_many({"_id": {"$in": inserted_ids}},
        #                            {"$set": {"ProjectId": new_project.get('project_id')}})
        
        collection_sam.update_many({}, {"$set": {"ProjectId": str(inserted_id)}})
        collection_deed.update_many({}, {"$set": {"ProjectId": str(inserted_id)}})

        background_tasks.add_task(run_scripts, str(new_project.id))
        return ProjectResponse(message = "project added successfully",result=new_project)

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/project/all', response_model=ProjectResponse, response_model_by_alias=False, response_description="Projects fetched successfully", status_code=status.HTTP_200_OK)
async def get_projects(
        payload: dict = Body(None, description="source"),
        page: int = Query(1, ge=1),
        page_size: int = Query(100, ge=1)):
    try:
        if not payload:
            source = 'all sources' 
            sort_order = 'last entry'

        else:
            source = payload.get('sourceType','all sources') 
            sort_order = payload.get('sortBy','last entry')

        collection_project = get_project_collection()

        filter_query = {}

        if source and str(source).lower() != "all sources":
            filter_query["source"] = str(source).lower()

        sort_direction = 1
        if sort_order and sort_order.lower() == "last entry":
            sort_direction = -1

        projects = collection_project.find(filter_query).sort("created_at", sort_direction).limit(page_size).skip((page - 1) * page_size)

        project_list = [project for project in projects]
        return ProjectResponse(message= "projects retrieved successfully", result= project_list,total= len(project_list))

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get('/project/{id}',response_model=ProjectResponse, response_model_by_alias=False, response_description="Project fetched successfully", status_code=status.HTTP_200_OK)
async def get_project_by_id(id: str):
    try:
        collection_project = get_project_collection()
        project = collection_project.find_one({"_id": ObjectId(id)})

        if project:
            project["_id"] = str(project["_id"])
            return ProjectResponse(message= "Project retrieved successfully", result= project, total=1)
        else:
            raise HTTPException(status_code=404, detail="Project not found for id: {}".format(id))

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@router.post('/file/unzip')
async def unzip_file(file: UploadFile = File(...)):
    zip_file = file.file  # Access the underlying SpooledTemporaryFile

    result_data = []  

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if not file_info.is_dir():
                file_name = file_info.filename
                content = zip_ref.read(file_name)
                # result_data.append((file_name, content))

            try:
                df = pd.read_csv(io.StringIO(content.decode("latin-1")))
                df = df.where(pd.notna(df), None)
                return df
                headers = df.columns.tolist()
                result_data.append({"msg": f"CSV file '{file_info.filename}' received", "data": df.to_dict(orient='records'),"headers": headers, "status_code": 200, "type": "csv"})
                break
            except ValueError:
                result_data.append({"msg": f"CSV file '{file_info.filename}' contains out-of-range float values", "status_code": 400, "type": "csv"})

    return result_data

    # with zipfile.ZipFile(zip_file,"r") as zf:
    #     zf.extractall()

    # return zf
#     with zip_ref.open(file_info) as file_obj:
#         contents = file_obj.read()

#         if file_info.filename.lower().endswith(".csv"):
#             try:
#                 df = pd.read_csv(io.StringIO(contents.decode("latin-1")))
#                 df = df.where(pd.notna(df), None)
#                 headers = df.columns.tolist()
#                 result_data.append({"msg": f"CSV file '{file_info.filename}' received", "data": df.to_dict(orient='records'), "headers": headers, "status_code": 200, "type": "csv"})
#             except ValueError:
#                 result_data.append({"msg": f"CSV file '{file_info.filename}' contains out-of-range float values", "status_code": 400, "type": "csv"})

#         elif file_info.filename.lower().endswith(".xlsx"):
#             df = pd.read_excel(io.BytesIO(contents))
#             df = df.where(pd.notna(df), None)
#             headers = df.columns.tolist()
#             json_data = df.to_json(orient='records', date_format='iso', default_handler=str)
#             result_data.append({"msg": f"XLSX file '{file_info.filename}' received", "data": json.loads(json_data), "status_code": 200, "type": "xlsx", "headers": headers})

# return result_data
