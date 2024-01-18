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
from utils import hash_password, unzip_file, verify_password, upload_file
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


def validate_sam_fields(companies: List[dict], companies_headers: List):
    fields_list = ['FIPSCode', 'PropertyFullStreetAddress', 'PropertyCityName', 'PropertyState', 'PropertyZipCode', 
                   'PropertyZip4', 'PropertyUnitType', 'PropertyUnitNumber', 'PropertyHouseNumber', 
                   'PropertyStreetDirectionLeft', 'PropertyStreetName', 'PropertyStreetSuffix', 'PropertyStreetDirectionRight', 
                   'PropertyAddressCarrierRoute', 'RecordType', 'RecordingDate', 'RecorderBookNumber', 'RecorderPageNumber', 
                   'RecorderDocumentNumber', 'APN', 'MultiAPNFlag', 'Borrower1FirstNameMiddleName', 'Borrower1LastNameOeCorporationName',
                   'Borrower1IDCode', 'Borrower2FirstNameMiddleName', 'Borrower2LastNameOrCorporationName', 'Borrower2IDCode',
                   'BorrowerVestingCode', 'LegalLotNumbers', 'LegalBlock', 'LegalSection', 'LegalDistrict', 'LegalLandLot', 'LegalUnit', 
                   'LegalCityTownshipMunicipality', 'LegalSubdivisionName', 'LegalPhaseNumber', 'LegalTractNumber', 'LegalBriefDescription', 
                   'LegalSectionTownshipRangeMeridian', 'LenderNameBeneficiary', 'LenderNameID', 'LenderType', 'LoanAmount', 'LoanType', 
                   'TypeFinancing', 'InterestRate', 'DueDate', 'AdjustableRateRider', 'AdjustableRateIndex', 'ChangeIndex', 
                   'RateChangeFrequency', 'InterestRateNotGreaterThan', 'InterestRateNotLessThan', 'MaximumInterestRate', 'InterestOnlyPeriod', 
                   'FixedStepConversionRateRider', 'FirstChangeDateYearConversionRider', 'FirstChangeDateMonthDayConversionRider', 
                   'PrepaymentRider', 'PrepaymentTermPenaltyRider', 'BuyerMailFullStreetAddress', 'BorrowerMailUnitType', 'BorrowerMailUnitNumber', 
                   'BorrowerMailCity', 'BorrowerMailState', 'BorrowerMailZipCode', 'BorrowerMailZip4', 'OriginalDateOfContract', 
                   'TitleCompanyName', 'LenderDBAName', 'LenderMailFullStreetAddress', 'LenderMailUnitType', 'LenderMailUnit', 
                   'LenderMailState', 'LenderMailZipCode', 'LenderMailZip4', 'LoanTermMonths', 'LoanTermYears', 'LoanNumber', 'PID', 
                   'AssessorLandUse', 'ResidentialIndicator', 'ConstructionLoan', 'CashPurchase', 'StandAloneRefi', 'EquityCreditLine', 
                   'PropertyUseCode', 'LoanTransactionType', 'MainRecordIDCode', 'LoanOrganizationNMLS_ID', 'LoanOrganizationName', 
                   'MortgageBrokerNMLS_ID', 'MortgageBroker', 'LoanOfficerNMLS_ID', 'LoanOfficerName', 'DPID', 'UpdateTimeStamp']
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
    fields_list = ['FIPSCode', 'PropertyFullStreetAddress', 'PropertyCityName', 'PropertyState', 'PropertyZipCode',
                    'PropertyZip4', 'PropertyUnitType', 'PropertyUnitNumber', 'PropertyHouseNumber', 'PropertyStreetDirectionLeft',
                    'PropertyStreetName', 'PropertyStreetSuffix', 'PropertyStreetDirectionRight', 'PropertyAddressCarrierRoute', 
                    'RecordingDate', 'RecorderBookNumber', 'RecorderPageNumber', 'RecorderDocumentNumber', 'DocumentTypeCode',
                    'APN', 'MultiAPNFlag', 'PartialInterestTransferred', 'Seller1FirstName&MiddleName', 'Seller1LastNameOrCorporation', 
                    'Seller1IDCode', 'Seller2FirstName&MiddleName', 'Seller2LastNameOrCorporation', 'Seller2IDCode', 
                    'Buyer1FirstName&MiddleName', 'Buyer1LastNameOrCorporation', 'Buyer1IDCode', 'Buyer2FirstName&MiddleName', 
                    'Buyer2LastNameOrCorporation', 'Buyer2IDCode', 'BuyerVestingCode', 'ConcurrentTDDocumentNumber', 'BuyerMailCity',
                    'BuyerMailState', 'BuyerMailZipCode', 'BuyerMailZip4', 'LegalLotCode', 'LegalLotNumber', 'LegalBlock', 
                    'LegalSection', 'LegalDistrict', 'LegalLandLot', 'LegalUnit', 'LegalCity', 'LegalSubdivisionName', 'LegalPhaseNumber',
                    'LegalTractNumber', 'LegalBriefDescription', 'LegalSectTownRangeMeridian', 'RecorderMapReference', 'BuyerMailingAddressCode', 
                    'PropertyUseCode', 'OriginalDateOfContract', 'SalesPrice', 'SalesPriceCode', 'CityTransferTax', 'CountyTransferTax', 
                    'TotalTransferTax', 'ConcurrentTDLenderName', 'ConcurrentTDLenderType', 'ConcurrentTDLoanAmount', 'ConcurrentTDLoanType', 
                    'ConcurrentTDTypeFinancing', 'ConcurrentTDInterestRate', 'ConcurrentTDDueDate', 'Concurrent2ndTDLoanAmount', 
                    'BuyerMailFullStreetAddress', 'BuyerMailUnitType', 'BuyerMailUnitNumber', 'PID', 'BuyerMailCareOfName', 
                    'TitleCompanyName', 'CompleteLegalDescriptionCode', 'AdjustableRateRider', 'AdjustableRateIndex', 'ChangeIndex', 
                    'RateChangeFrequency', 'InterestRateNotGreaterThan', 'InterestRateNotLessThan', 'MaximumInterestRate', 'InterestOnlyPeriod', 
                    'FixedStepRateRider', 'FirstChangeDateYear', 'FirstChangeDateMonth&Day', 'PrepaymentRider', 'PrepaymentTerm', 
                    'AssessorLandUse', 'ResidentialIndicator', 'ConstructionLoan', 'InterFamily', 'CashPurchase', 'StandAloneRefi', 
                    'EquityCreditLine', 'REOFlag', 'DistressedSaleFlag', 'SellerMailAddressFullStreet', 'SellerMailAddressUnitType', 
                    'SellerMailAddressUnitNumber', 'SellerMailAddressCityName', 'SellerMailAddressStateCode', 'SellerMailAddressZipCode', 
                    'SellerMailAddressZip4', 'DeedTransactionType', 'LoanTransactionType', 'ShortSaleFlag', 'MainRecordIDCode', 
                    'LoanOrganizationNMLS_ID', 'LoanOrganizationName', 'MortgageBrokerNMLS_ID', 'MortgageBroker', 'LoanOfficerNMLS_ID', 
                    'LoanOfficerName', 'DPID', 'UpdateTimeStamp']
    # print(companies_headers)
    missing_fields = check_missing_fields(companies_headers, fields_list)
    if missing_fields:
        return {"msg": "invalid_deed_file", "missing_field": missing_fields, "is_valid": False}
    
    return {"msg": "file validated successfully", "is_valid": True}



@router.post('/validate')
async def validate_file(api_key: str = Body(None), source: str = Body(None), file: UploadFile = File(None)):
    try:

        if file:
            validation_results = {}

            if file.filename.endswith('.zip'):
                response = await unzip_file(file)

                sam_response = response.get("data",{}).get("sam",{})
                deed_response = response.get("data",{}).get("deed",{})

                if sam_response or deed_response is not None:

                    if sam_response and sam_response.get("status_code") == 200:

                        if sam_response.get("type") == "csv":
                            sam_transactions = sam_response.get('data', [])
                            sam_transactions_headers = sam_response.get('headers', [])

                            if not sam_transactions:
                                validation_results["sam"] = {"msg": "No Sam Transaction Provided in request"}
                            else:
                                invalid_data = validate_sam_fields(sam_transactions, sam_transactions_headers) 
                                if invalid_data.get('missing_field') or invalid_data.get('invalid_transactions'):
                                    validation_results["sam"] = invalid_data

                    if deed_response and deed_response.get("status_code") == 200:

                        if deed_response.get("type") == "csv":
                            deed_transactions = deed_response.get('data', [])
                            deed_transactions_headers = sam_response.get('headers', [])

                            if not deed_transactions:
                                validation_results["deed"] = {"msg": "No Deed Transaction Provided in request"}
                            else:
                                invalid_data = validate_deed_fields(deed_transactions, deed_transactions_headers)
                                if invalid_data.get('missing_field') or invalid_data.get('invalid_transactions'):
                                    validation_results["deed"] = invalid_data


            elif file:
                response = await upload_file(file)
                companies = []
                companies_headers = []
                if response.get("status_code") == 200:
                    if response.get("type") == "json":
                        companies = response.get('data', dict())

                    elif response.get("type") == "csv" or response.get("type") == "xlsx":
                        companies = response.get('data', [])
                        companies_headers = response.get('headers', [])

                if not companies:
                    validation_results["companies"] = {"msg": "No Companies Provided in request"}
                else:
                    invalid_data = validate_sam_fields(companies, companies_headers)
                    if invalid_data.get('missing_field') or invalid_data.get('invalid_transactions'):
                        validation_results["companies"] = invalid_data

            if validation_results:
                return {"msg": "Invalid File", "validation_results": validation_results, "is_valid": False}
            else:
                return {"msg": "All data validated successfully", "is_valid": True}


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
