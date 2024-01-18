from datetime import datetime
from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body, Query, BackgroundTasks, UploadFile, File, status, Response
import subprocess
from pymongo import MongoClient
from pymongo.collection import Collection
import asyncio
from Oauth import get_current_user, create_access_token
from config.db import get_collection
from data import load_json
from schemas import UserBaseSchema
from schemas.project import ProjectSchema,ProjectResponse
from utils import hash_password, verify_password, upload_file, unzip_file
import os
import zipfile
import pandas as pd
import io
import json
import numpy as np
import re


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

def run_scripts(id:str,sam_inserted_ids:list,deed_inserted_ids:list):

    project_id = id
    cur_dir = os.getcwd()
    script_directory = os.path.join(cur_dir, "scripts")

    script_filename = "master_script.py"  
    script_path = f"{script_directory}/{script_filename}"
    subprocess.run(["python", script_path, "--project_id", project_id])

    # subprocess.run(["python", script_path, "--project_id", project_id, "--deed_inserted_ids", ",".join(deed_inserted_ids),"--sam_inserted_ids", ",".join(sam_inserted_ids)])

async def update_collection_async(collection, ids, project_id):
    if ids:
        collection.update_many({"_id": {"$in": ids}}, {"$set": {"ProjectId": str(project_id)}})

    
@router.post('/project', response_model=ProjectResponse, response_model_by_alias=False, response_description="Project added successfully", status_code=status.HTTP_201_CREATED)
async def create_project(background_tasks: BackgroundTasks, file: UploadFile = File(None),
                         user: UserBaseSchema = Depends(get_current_user), project: ProjectSchema = None):
    try:

        collection_sam = get_sam_collection()
        collection_deed = get_deed_collection()
        result_sam = []
        companies = []
        sam_inserted_ids = []
        deed_inserted_ids = []

        try:
            if file.filename.endswith('.zip'):
                response = await unzip_file(file)

                sam_response = response.get("data",{}).get("sam",{})
                deed_response = response.get("data",{}).get("deed",{})

                if sam_response or deed_response is not None:

                    if sam_response and sam_response.get("status_code") == 200:

                        if sam_response.get("type") == "csv":
                            sam_transactions = sam_response.get('data', [])

                        if not sam_transactions:
                            return {"msg": "No Sam Transaction Provided in request"}

                        result_sam = collection_sam.insert_many(sam_transactions)

                    if deed_response and deed_response.get("status_code") == 200:

                        if deed_response.get("type") == "csv":
                            deed_transactions = deed_response.get('data', [])

                        if not deed_transactions:
                            return {"msg": "No Deed Transaction Provided in request"}

                        result_deed = collection_deed.insert_many(deed_transactions)


                else:
                    cur_dir = os.getcwd()
                    sam_file_path = os.path.join(cur_dir, "data/sam.json")
                    deed_file_path = os.path.join(cur_dir, "data/deeds.json")
                    sam_transactions = load_json.load_data_from_json(sam_file_path)
                    deed_transactions = load_json.load_data_from_json(deed_file_path)

                    result_sam = collection_sam.insert_many(sam_transactions)
                    result_deed = collection_deed.insert_many(deed_transactions)

                sam_inserted_ids = result_sam.inserted_ids
                deed_inserted_ids = result_deed.inserted_ids

            elif file:
                response = await upload_file(file)

                if response.get("status_code") == 200:
                    if response.get("type") == "json":
                        companies = response.get('data', dict())

                    elif response.get("type") == "csv" or response.get("type") == "xlsx":
                        companies = response.get('data', [])

                    if not companies:
                        return {"msg": "No Companies Provided in request"}

                    result_sam = collection_sam.insert_many(companies)

                else:
                    cur_dir = os.getcwd()
                    sam_file_path = os.path.join(cur_dir, "data/sam.json")
                    companies = load_json.load_data_from_json(sam_file_path)
                    result_sam = collection_sam.insert_many(companies)

                sam_inserted_ids = result_sam.inserted_ids

        except Exception as e:
            raise HTTPException(status_code=400, detail=f"file not uploaded {str(e)}")

        collection_project = get_project_collection()
        new_project = {
            "id": ObjectId(),
            "user_email": user.get('email'),
            "total_mortgage_transaction": {'sam_transactions': len(sam_transactions), 'deed_transactions': len(deed_transactions)},
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
        project_id = result_project.inserted_id

        if file.filename.endswith('.zip'):
            await asyncio.gather(
                update_collection_async(collection_sam, sam_inserted_ids,str(new_project.id)),
                update_collection_async(collection_deed,deed_inserted_ids,str(new_project.id))
            )
        #took 0.22 sec for 5k*2 updates
        #todo : for 1million updates, will take approx 25 sec , so need to run this in script as well
        else:
            await asyncio.gather(
                update_collection_async(collection_sam, sam_inserted_ids,str(new_project.id))
            )

        background_tasks.add_task(run_scripts, str(new_project.id), sam_inserted_ids, deed_inserted_ids)
        return ProjectResponse(message = "project added successfully",result=new_project, total=1)

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
            return ProjectResponse(message= "Project retrieved successfully", result= project, total=1)
        else:
            raise HTTPException(status_code=404, detail="Project not found for id: {}".format(id))

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



def convert_to_serializable(value):
    if isinstance(value, np.floating) and (np.isnan(value) or np.isinf(value)):
        return None
    return value

def convert_to_json_compliant(data):
    return json.dumps(data, default=convert_to_serializable)

@router.post('/file/unzip')
async def unzip_file(file: UploadFile = File(...)):
    zip_file = file.file  # Access the underlying SpooledTemporaryFile

    result_data = {}

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if not file_info.is_dir():
                file_name = file_info.filename
                print(file_name)
                content = zip_ref.read(file_name)

                file_src = re.search(r"sam|deed", file_name)
                if file_src:
                    file_src = file_src.group()
                else:
                    file_src = "sam"
                try:
                    df = pd.read_csv(io.StringIO(content.decode("latin-1")))
                    df = df.map(convert_to_serializable)
                    df = df.fillna('')
                    headers = df.columns.tolist()

                    if file_src not in result_data:
                        result_data[file_src]= {
                            "msg": f"CSV file '{file_info.filename}' received",
                            "data": json.loads(convert_to_json_compliant(df.to_dict(orient='records'))),
                            "headers": headers,
                            "status_code": 200,                  
                            "type": "csv",
                            "file_name": file_info.filename
                        }
                   
                except ValueError:
                    result_data["error"] = {
                        "msg": f"CSV file '{file_info.filename}' contains out-of-range float values",
                        "status_code": 400
                    }
    return {"msg": f"CSV file {file_info.filename}","data": result_data}
    # response_content = jsonable_encoder(result_data)
    # return JSONResponse(content=response_content, media_type="application/json")


