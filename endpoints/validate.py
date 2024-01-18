from datetime import datetime
from typing import List
import json
from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body, UploadFile, File
from pydantic import ValidationError
from pymongo import MongoClient
import os
from schemas.sam import PropertyData
from utils import hash_password, verify_password, upload_file
import traceback

router = APIRouter(
    prefix="/project",
    tags=["Project"],
    responses={404: {"description": "Not found"}},
)

_ = load_dotenv(find_dotenv())
mongo_url = os.getenv("MONGO_URL")


def get_api_key_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    api_key_collection = db["api_key"]
    return api_key_collection


def handle_non_serializable(obj):
    if isinstance(obj, float) and (obj == float('inf') or obj == float('-inf') or obj != obj):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def check_missing_fields(companies_headers, fields_list):
    missing_field = [
        value for value in fields_list if value not in companies_headers]
    return missing_field


def validate_fields(companies: List[dict], companies_headers: List):
    fields_list = [
        "FIPSCode", "PropertyZipCode", "PropertyZip4", "PropertyUnitNumber",
        "PropertyHouseNumber", "RecordingDate", "RecorderBookNumber", "RecorderPageNumber",
        "RecorderDocumentNumber", "APN", "MultiAPNFlag", "LegalBlock", "LegalSection",
        "LegalDistrict", "LegalLandLot", "LegalUnit", "LegalPhaseNumber", "LegalTractNumber",
        "LenderNameID", "DueDate", "AdjustableRateRider", "FirstChangeDateYearConversionRider",
        "FirstChangeDateMonthDayConversionRider", "PrepaymentRider", "PrepaymentTermPenaltyRider",
        "BorrowerMailUnitNumber", "BorrowerMailZipCode", "BorrowerMailZip4", "OriginalDateOfContract",
        "LenderMailFullStreetAddress", "LenderMailZipCode", "LenderMailZip4", "LoanTermMonths",
        "LoanTermYears", "AssessorLandUse", "LoanTransactionType", "LoanOrganizationNMLS_ID",
        "MortgageBrokerNMLS_ID", "LoanOfficerNMLS_ID", "DPID", "UpdateTimeStamp"
    ]
    # print(companies_headers)
    missing_fields = check_missing_fields(companies_headers, fields_list)
    if missing_fields:
        return {"msg": "invalid_file", "missing_field": missing_fields, "is_valid": False}

    invalid_transactions = []
    for company in companies:
        try:
            PropertyData(**company)

        except ValidationError as e:
            invalid_transactions.append(company)

    return {"msg": "file validated successfully", "invalid_transactions": invalid_transactions, "is_valid": True}

def validate_deed_fields(companies: List[dict], companies_headers: List):
    fields_list = [
        "FIPSCode", "PropertyZipCode", "PropertyZip4", "PropertyUnitNumber",
        "PropertyHouseNumber", "RecordingDate", "RecorderBookNumber", "RecorderPageNumber",
        "RecorderDocumentNumber", "APN", "MultiAPNFlag", "LegalBlock", "LegalSection",
        "LegalDistrict", "LegalLandLot", "LegalUnit", "LegalPhaseNumber", "LegalTractNumber",
        "LenderNameID", "DueDate", "AdjustableRateRider", "FirstChangeDateYearConversionRider",
        "FirstChangeDateMonthDayConversionRider", "PrepaymentRider", "PrepaymentTermPenaltyRider",
        "BorrowerMailUnitNumber", "BorrowerMailZipCode", "BorrowerMailZip4", "OriginalDateOfContract",
        "LenderMailFullStreetAddress", "LenderMailZipCode", "LenderMailZip4", "LoanTermMonths",
        "LoanTermYears", "AssessorLandUse", "LoanTransactionType", "LoanOrganizationNMLS_ID",
        "MortgageBrokerNMLS_ID", "LoanOfficerNMLS_ID", "DPID", "UpdateTimeStamp"
    ]
    # print(companies_headers)
    missing_fields = check_missing_fields(companies_headers, fields_list)
    if missing_fields:
        return {"msg": "invalid_file", "missing_field": missing_fields, "is_valid": False}

    invalid_transactions = []
    for company in companies:
        try:
            PropertyData(**company)

        except ValidationError as e:
            invalid_transactions.append(company)

    return {"msg": "file validated successfully", "invalid_transactions": invalid_transactions, "is_valid": True}



@router.post('/validate')
async def validate_file(api_key: str = Body(None), source: str = Body(None), file: UploadFile = File(None)):
    try:
        if file:
            response = await upload_file(file)
            companies = []
            companies_headers = []
            if response.get("status_code") == 200:
                if response.get("type") == "json":
                    companies = response.get('data', dict())

                elif response.get("type") == "csv":
                    companies = response.get('data', [])
                    companies_headers = response.get('headers', [])

                elif response.get("type") == "xlsx":
                    companies = response.get('data', [])
                    companies_headers = response.get('headers', [])

            if not companies:
                return {"msg": "No Companies Provided in request"}

            invalid_data = validate_fields(companies, companies_headers)
            if invalid_data.get('missing_field'):
                return invalid_data

            if invalid_data.get('error_details'):
                return invalid_data

            return {"msg": "file validated successfully", "is_valid": True}

        elif api_key:
            if str(source).lower() == "forecasa" and api_key == "fNc4oVFWFjx1SZX9YdI0MRzWaE3Jlh7":
                return {"msg": "forecasa_api_key is valid", "is_valid": True}

            elif str(source).lower() == "blackknight" and api_key == "b2Gz8tO0YsFb1v6iPpTmAj9KkH7hJqLx":
                return {"msg": "blacknight_api_key is valid", "is_valid": True}

            else:
                return {"msg": "invalid api key", "is_valid": False}

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))


@router.post('/api_key')
async def create_api_key(api_key: str = Body(None), source: str = Body(None)):
    try:
        api_key_collection = get_api_key_collection()
        if (str(source).lower() == "forecasa" and api_key == "fNc4oVFWFjx1SZX9YdI0MRzWaE3Jlh7") or \
                (str(source).lower() == "blacknight" and api_key == "b2Gz8tO0YsFb1v6iPpTmAj9KkH7hJqLx"
                 ):
            query = {"api_key": api_key, "source": str(source).lower()}
            existing_key = api_key_collection.find_one(query)
            if existing_key:
                return {"msg": "key already exists"}
            result_api_key = api_key_collection.insert_one(query)
            return {"msg": "key successfully inserted"}

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
