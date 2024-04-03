from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body, status, Query
from pymongo import MongoClient
from pymongo.collection import Collection
from bson import ObjectId
from datetime import datetime
from config.db import get_collection
import os
import traceback
from schemas.organization import OrganizationResponse, OrganizationUpdateSchema,AddOrganizationSchema

router = APIRouter(
    # prefix="/organization",
    tags=["Organization"],
    responses={404: {"description": "Not found"}},
)

_ = load_dotenv(find_dotenv())
mongo_url = os.getenv("mongo_url")

def get_organization_collection():
    client = MongoClient(mongo_url)
    db = client["harry"]
    organization_collection = db["organization"]
    return organization_collection

def get_people_collection():
    client = MongoClient(mongo_url)
    db = client["harry"]
    organization_collection = db["people"]
    return organization_collection

def get_contact_collection():
    client = MongoClient(mongo_url)
    db = client["harry"]
    organization_collection = db["contact"]
    return organization_collection

def get_user_collection():
    client = MongoClient(mongo_url)
    db = client["harry"]
    organization_collection = db["user"]
    return organization_collection

@router.post('/organization', response_model_by_alias=False, response_description="Project added successfully", status_code=status.HTTP_201_CREATED)
async def create_organization(organization: AddOrganizationSchema = None):
    try:
        collection_organization = get_organization_collection()
        collection_people = get_people_collection()
        collection_user = get_user_collection()
        collection_contact = get_contact_collection()
        user = collection_user.find_one({"email": organization.user_email})
        existing_organization = collection_organization.find_one({"name": organization.name})

        if existing_organization:
            raise HTTPException(status_code=404, detail="Organization already exists with this name.")
        
        new_organization = {
            "user_id": user['_id'],
            "name": organization.name,
            "address": organization.address,
            "annual_revenue":organization.annual_revenue,
            "growth_from_last_year": organization.growth_from_last_year,
            "team_size": int(organization.team_size),
            "official_phone": organization.official_phone,
            "website": organization.website,
            "county": organization.county,
            "state": organization.state,
            "last_modified": datetime.now(),
            "created_at": datetime.now()
        }

        result_organization = collection_organization.insert_one(new_organization)
        for decision_maker in organization.decisionMakers:
            new_decision_maker = {
                "user_id": user["_id"],
                "organization_id": result_organization.inserted_id,
                "name": decision_maker.name,
                "title": decision_maker.title,
                "linkedin": decision_maker.linkedin,
                "created_at": datetime.now(),
                "last_modified": datetime.now()
            }
            result_people= collection_people.insert_one(new_decision_maker)
            if decision_maker.contact:
                for phone in decision_maker.contact:
                    new_contact = {
                        "user_id": user["_id"],
                        "people_id": result_people.inserted_id,
                        "value": phone.value,
                        "type": "phone",
                        "created_at": datetime.now(),
                        "last_modified": datetime.now()
                    }
                    collection_contact.insert_one(new_contact)
            if decision_maker.emails:
                for email in decision_maker.emails:
                    new_contact = {
                        "user_id": user["_id"],
                        "people_id": result_people.inserted_id,
                        "value": email.value,
                        "type": "email",
                        "created_at": datetime.now(),
                        "last_modified": datetime.now()
                    }
                    collection_contact.insert_one(new_contact)

        return OrganizationResponse(message = "organization added successfully",result=new_organization, total=1)

    except HTTPException as http_exception:
        raise http_exception

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/organization', response_description="List of organizations", response_model=OrganizationResponse, response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def get_organization_by_id(
        id: str = Query(...),
        page_size: int = Query(10, ge=1),
        page: int = Query(1, ge=1)):
    try:
        collection_organization = get_organization_collection()

        existing_organization = collection_organization.find_one({"_id": ObjectId(id)})

        if not existing_organization:
            raise HTTPException(status_code=404, detail="Organization not found")
        # filter_query = {"_id": ObjectId(id)}

        # organization = collection_organization.find(filter_query).limit(page_size).skip((page - 1) * page_size)

        return OrganizationResponse(result=existing_organization, total=1, message="Organization retrieved successfully")

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get('/organization/all', response_description="List of organizations", response_model=OrganizationResponse, response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def get_organizations(
        page_size: int = Query(10, ge=1),
        page: int = Query(1, ge=1)):
    try:
        collection_organization = get_organization_collection()
        collection_people = get_people_collection()

        filter_query = {}

        organization = list(collection_organization.find(filter_query).limit(page_size).skip((page - 1) * page_size))

        for org in organization:
            org['created_at'] = org['created_at'].strftime("%Y-%m-%d %H:%M:%S")
            org['last_modified'] = org['last_modified'].strftime("%Y-%m-%d %H:%M:%S")
            org['total_decision_maker'] = collection_people.count_documents({"organization_id": org['_id']})
        return OrganizationResponse(result=organization, total=len(organization), message="Organizations retrieved successfully")

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    



#write api to update organization
@router.put('/organization', response_model_by_alias=False, response_description="Organization updated successfully")
async def update_organization(organization_data: OrganizationUpdateSchema):
    try:
            
        collection_organization = get_organization_collection()
        organization = collection_organization.find_one({"_id": ObjectId(organization_data.id)})

        if not organization:
            raise HTTPException(status_code=404, detail=f'No organization with this id: {organization_data.id} found')

        updated_organization_data = {
            k: v for k, v in organization_data.model_dump(by_alias=True,exclude={"id"}).items() if v is not None
        }
        print(updated_organization_data)
        update_result = collection_organization.find_one_and_update(
            {"_id": ObjectId(organization_data.id)},
            {"$set": updated_organization_data}
        )

        return {"msg": "Organization updated successfully"} 
    
    except HTTPException as http_exception:
        raise http_exception
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



    

    
