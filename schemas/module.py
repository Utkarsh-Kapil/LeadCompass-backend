from enum import Enum

from pydantic import ConfigDict, BaseModel, EmailStr, constr, Field
from datetime import datetime
from typing import Optional, Annotated, List, Union
from Enum import StatusEnum
from pydantic.functional_validators import BeforeValidator


class AmountOptions(str, Enum):
    ALL_AMOUNTS = "All Amounts"
    LESS_THAN_100K = "Less than $100,000"
    GREATER_THAN_100K = "Greater than $100,000"


class TransactionCountOptions(str, Enum):
    ALL_TRANSACTIONS = "All Transactions"
    GREATER_THAN_1 = "Greater than 1"
    LESS_THAN_10 = "Less than 10"
    GREATER_THAN_10 = "Greater than 10"


class DurationOptions(str, Enum):
    ALL_DURATIONS = "All Durations"
    LESS_THAN_6_MONTHS = "Less than 6 months"
    LESS_THAN_1_YEAR = "Less than 1 year"
    GREATER_THAN_1_YEAR = "Greater than 1 year"

class PrefixedValues(BaseModel):
    prefix: str
    value: Optional[int] = None

class TransactionFilters(BaseModel):
    amount: Optional[PrefixedValues] = None
    transaction_count: Optional[PrefixedValues] = None
    transaction_year: Optional[PrefixedValues] = None
    borrower_type: Optional[str] = None
    states: Optional[list[str]] = []
    county_codes: Optional[list[str]] = []


# Represents an ObjectId field in the database.
# It will be represented as a `str` on the model so that it can be serialized to JSON.
PyObjectId = Annotated[str, BeforeValidator(str)]


class ModuleSchema(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    project_id: str
    name: str
    status: str
    project: Optional[dict] = None
    user_id: str
    user_email: str
    filters: Optional[dict] = None
    filtered_documents_count: int
    filtered_documents_untag_count: int
    already_taged_documents_count: int
    total_partial_loan_amount: int
    monthly_transactions_for_past_12_months: Optional[List[dict]] = None
    total_loan_amount: Optional[int] = None
    total_loan_count: Optional[int] = None
    total_properties: Optional[int] = None
    timeline: Optional[dict] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

class ModuleResponse(BaseModel):
    message: str
    total: Optional[int] = None
    result: Union[ModuleSchema, List[ModuleSchema]]
