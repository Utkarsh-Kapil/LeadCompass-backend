import io
import json
import os
import zipfile
import pandas as pd
from fastapi import Depends, HTTPException, status, File, UploadFile
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext

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





async def unzip_file(file: UploadFile = File(...)):
    zip_file = file

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        result_data = []  

        for file_name in zip_ref.namelist():
            contents = zip_ref.read(file_name)

            if file_name.lower().endswith(".csv"):
                df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
                df = df.where(pd.notna(df), None)
                headers = df.columns.tolist()
                result_data.append({"msg": f"CSV file '{file_name}' received", "data": df.to_dict(orient='records'), "headers": headers, "status_code": 200, "type": "csv"})

            elif file_name.lower().endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(contents))
                df = df.where(pd.notna(df), None)
                headers = df.columns.tolist()
                json_data = df.to_json(orient='records', date_format='iso', default_handler=str)
                result_data.append({"msg": f"XLSX file '{file_name}' received", "data": json.loads(json_data), "status_code": 200, "type": "xlsx", "headers": headers})

        return result_data