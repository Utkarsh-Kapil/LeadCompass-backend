import io
import json
import os
import zipfile
import pandas as pd
from fastapi import Depends, HTTPException, status, File, UploadFile
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext
import numpy as np
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import re

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str):
    return pwd_context.hash(password)


def verify_password(password: str, hashed_password: str):
    return pwd_context.verify(password, hashed_password)


def authenticate_user(credentials: HTTPBasicCredentials = Depends(HTTPBasic())):
    """
    Authenticate User with Basic Authentication

    This function is used for authenticating a user with Basic Authentication. It checks the provided HTTPBasicCredentials
    against the stored username and password. If the credentials are valid, the function returns the authenticated username.

    Parameters:
        - `credentials`: HTTPBasicCredentials - The provided username and password for authentication.

    Returns:
        - If authentication is successful, returns the authenticated username.
        - If the provided credentials are invalid, raises an HTTPException with a 401 Unauthorized status.
    """

    username = os.getenv("username")
    password = os.getenv("password")

    if credentials.username != username or credentials.password != password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


async def upload_file(file: UploadFile = File(...)):
    file_extension = file.filename.split(".")[-1]

    if file_extension.lower() == "json":
        contents = await file.read()
        try:
            json_data = json.loads(contents.decode("utf-8"))
            return {"msg": "JSON file received", "data": json_data, "status_code": 200, "type": "json"}
        except json.JSONDecodeError:
            pass

    elif file_extension.lower() in ["csv", "xlsx"]:
        contents = await file.read()
        try:
            if file_extension.lower() == "csv":
                df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
                df = df.where(pd.notna(df), None)
                headers = df.columns.tolist()
                return {"msg": "CSV file received", "data": df.to_dict(orient='records'), "headers": headers,
                        "status_code": 200,
                        "type": "csv"}

            elif file_extension.lower() == "xlsx":
                df = pd.read_excel(io.BytesIO(contents))
                df = df.where(pd.notna(df), None)
                headers = df.columns.tolist()
                json_data = df.to_json(orient='records', date_format='iso', default_handler=str)
                return {"msg": "xlxs file received", "data": json.loads(json_data), "status_code": 200, "type": "xlsx", "headers": headers}


        except pd.errors.ParserError:
            pass

    return {"msg": "Unsupported file format, not csv/xlxs/json"}



def convert_to_serializable(value):
    if isinstance(value, np.floating) and (np.isnan(value) or np.isinf(value)):
        return None
    return value

def convert_to_json_compliant(data):
    return json.dumps(data, default=convert_to_serializable)

async def unzip_file(file: UploadFile = File(...)):
    zip_file = file.file 
    uploaded_file = set()
    result_data = {}
    is_valid = False

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if not file_info.is_dir():
                file_name = file_info.filename
                print(file_name)
                content = zip_ref.read(file_name)

                file_src = re.search(r"sam|deed", file_name)
                if file_src:
                    file_src = file_src.group()
                    # print(f"uk_debug3_{file_src}")
                elif file_src != "sam" or file_src != "deed":
                    continue
                
                uploaded_file.add(file_src)
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
                    else:
                        existing_entry = result_data[file_src]
                        if len(existing_entry["data"])==0:
                            existing_entry["data"] = json.loads(convert_to_json_compliant(df.to_dict(orient='records')))
                    
                    is_valid = True

                except ValueError:
                    result_data["error"] = {
                        "msg": f"CSV file '{file_info.filename}' contains out-of-range float values",
                        "status_code": 400
              }
                    
    if not is_valid: 
        return {"msg": "file_name should have either sam or deed","is_valid":False}   
    return {"msg": f"CSV file {file_info.filename}","data": result_data,"is_valid":True,"uploaded_file":list(uploaded_file)}
    