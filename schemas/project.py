from enum import Enum
from pydantic import ConfigDict, BaseModel, EmailStr, constr, Field
from datetime import datetime
from typing import Optional, Annotated, List, Union
from Enum import ContactEnum
from pydantic.functional_validators import BeforeValidator


# Represents an ObjectId field in the database.
# It will be represented as a `str` on the model so that it can be serialized to JSON.
PyObjectId = Annotated[str, BeforeValidator(str)]


class ProjectSchema(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    user_email: EmailStr
    total_mortgage_transaction: Optional[int] = None
    last_10_year_transactions_mortgage: Optional[int] = None
    residential_properties_transactions_mortgage: Optional[int] = None
    status: Optional[str] = ""
    source: Optional[str] = ""
    project_name: Optional[str] = ""
    created_at: datetime = None

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

class ProjectUpdateSchema(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
class ProjectResponse(BaseModel):
    message: str
    total: Optional[int] = None
    result: Union[ProjectSchema, List[ProjectSchema]]
