from datetime import datetime
from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from fastapi import APIRouter, HTTPException, Depends, Body, Query,status
from pymongo import MongoClient
from pymongo.collection import Collection
from fastapi.encoders import jsonable_encoder
from typing import Optional
from Oauth import get_current_user
from schemas import UserBaseSchema
from schemas.contact import ContactSchema,ContactResponse
import os
import traceback

router = APIRouter(
    prefix="/contact",
    tags=["Contact"],
    responses={404: {"description": "Not found"}},
)

_ = load_dotenv(find_dotenv())
mongo_url = os.getenv("MONGO_URL")


def get_contact_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    contact_collection = db["contact"]
    return contact_collection


def get_project_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["lead_compass"]
    project_collection = db["project"]
    return project_collection


@router.post('/add', response_model=ContactResponse, response_model_by_alias=False, response_description="Contact added successfully", status_code=status.HTTP_201_CREATED)
async def create_contact(contact: ContactSchema):
    try:
        collection_contact = get_contact_collection()
        existing_contact = collection_contact.find_one({"_id": ObjectId(contact.id)})

        if existing_contact:
            raise HTTPException(status_code=400, detail="Contact already exists for id: {}".format(contact.id))
        

        contact_obj = {
            "first_name": contact.first_name,
            "last_name": contact.last_name,
            "email": contact.email,
            "primary_contact": contact.primary_contact,
            "secondary_contact": contact.secondary_contact,
            "linkedIn": contact.linkedIn,
            "contact_type": contact.contact_type,
            "company_id": contact.company_id,
            "created_at": datetime.now(),
        }

        contact_obj = ContactSchema(**contact_obj)

        response = collection_contact.insert_one(contact_obj.model_dump(by_alias=True, exclude=["id"]))
       
        new_contact = collection_contact.find_one({"_id": response.inserted_id})

        return ContactResponse(result=new_contact, message="Contact added successfully for  id: {}".format(response.inserted_id))

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/all', response_description="List of contacts", response_model=ContactResponse, response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def get_contacts(
        page_size: int = Query(10, ge=1),
        page: int = Query(1, ge=1),
        sort_by: str = Query(None)):
    try:
        collection_contact = get_contact_collection()
        
        sort = -1 if str(sort_by).lower() == "last entry" else 1

        contacts = list(collection_contact.find({}).sort("created_at", sort).limit(page_size).skip((page - 1) * page_size))

        return ContactResponse(result=contacts, total=collection_contact.count_documents({}), message="Contacts retrieved successfully")

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/{id}', response_description="Single Contact",response_model=ContactResponse, response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def get_contact(id: str):
    try:
        collection_contact = get_contact_collection()

        existing_contact = collection_contact.find_one({"_id": ObjectId(id)})

        if not existing_contact:
            raise HTTPException(status_code=404, detail="Contact not found for id: {}".format(id))

        return ContactResponse(result=existing_contact, message="Contact retrieved successfully for id: {}".format(id))

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    

@router.put('/{id}', response_description="Single Contact update", response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def update_contact(id: str, contact: ContactSchema):
    try:
        collection_contact = get_contact_collection()

        existing_contact = collection_contact.find_one({"_id": ObjectId(id)})

        if not existing_contact:
            raise HTTPException(status_code=404, detail="Contact not found for id: {}".format(id))
        
        updated_contact = {
            k: v for k, v in contact.model_dump(by_alias=True).items() if v is not None
        }
        update_result = collection_contact.find_one_and_update(
            {"_id": ObjectId(id)},
            {"$set": updated_contact}
        )

        return {"msg": "Contact updated successfully for id: {}".format(id)}

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete('/{id}', response_description="Single Contact delete", response_model_by_alias=False, status_code=status.HTTP_200_OK)
async def delete_contact(id: str):
    print(id)
    try:
        collection_contact = get_contact_collection()

        existing_contact = collection_contact.find_one({"_id": ObjectId(id)})

        if not existing_contact:
            raise HTTPException(status_code=404, detail="Contact not found for id: {}".format(id))
        
        result = collection_contact.delete_one({"_id": ObjectId(id)})

        return {"msg":"Contact deleted successfully for id: {}".format(id)}

    except HTTPException as http_exception:
        traceback.print_exc()
        raise http_exception

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
