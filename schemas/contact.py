from enum import Enum
from pydantic import ConfigDict, BaseModel, EmailStr, constr, Field
from datetime import datetime
from typing import Optional, Annotated, List, Union
from Enum import ContactEnum
from pydantic.functional_validators import BeforeValidator


# Represents an ObjectId field in the database.
# It will be represented as a `str` on the model so that it can be serialized to JSON.
PyObjectId = Annotated[str, BeforeValidator(str)]


class ContactSchema(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    first_name: Optional[str] = ""
    last_name: Optional[str] = ""
    email: EmailStr
    primary_contact: Optional[str] = ""
    secondary_contact: Optional[str] = ""
    linkedIn: Optional[str] = ""
    created_at: datetime = None
    contact_type : ContactEnum = "primary"
    company_id : Optional[int] = None

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

class ContactUpdateSchema(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
class ContactResponse(BaseModel):
    message: str
    total: Optional[int] = None
    result: Union[ContactSchema, List[ContactSchema]]
