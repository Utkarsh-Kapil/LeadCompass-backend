from typing import List, Optional, Annotated, Union
from pydantic import BaseModel, Field
from pydantic.functional_validators import BeforeValidator

PyObjectId = Annotated[str, BeforeValidator(str)]

class Transaction(BaseModel):
    FIPSCode: int
    PropertyFullStreetAddress: str
    PropertyCityName: str
    PropertyState: str
    PropertyZipCode: int
    PropertyZip4: int
    PropertyUnitType: str
    PropertyUnitNumber: int
    APN: Union[str,int] 
    LenderNameBeneficiary: str
    LoanAmount: int
    BuyerMailFullStreetAddress: str
    OriginalDateOfContract: int
    DPID: Union[str,int]
    RcCalPartialLoanAmount: int
    TransactionId: Union[str,int] 
    BuyerMailZipCode: int
    BuyerMailZip4: int
    BuyerMailUnitType: str
    BuyerMailUnitNumber: int
    BuyerMailState: str
    BuyerMailCity: str
    RcCalSource: str
    ProjectId: int
    RcCalType: str

class GroupedTransaction(BaseModel):
    DPID: Union[str,int]
    total_loan_count: int
    total_loan_amount: int
    lenders_name: List[str]
    transactions: List[Transaction]

class ProspectModel(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    RcCalBorrower: str
    RcCalTotalLoanAmount: int
    RcCalNumberOfLoans: int
    PropertyFullStreetAddress: dict
    PropertyState: dict
    PropertyCityName: dict
    PropertyZipCode: dict
    PropertyZip4: dict
    PropertyUnitType: dict
    PropertyUnitNumber: dict
    BuyerMailFullStreetAddress: dict
    BuyerMailCity: dict
    BuyerMailState: dict
    BuyerMailZipCode: dict
    BuyerMailZip4: dict
    BuyerMailUnitType: dict
    BuyerMailUnitNumber: dict
    RcCalType: dict
    DPID: dict
    RcCalSource: dict
    OriginalDateOfContract: dict
    LenderNameBeneficiary: dict
    RcCalLatestTransactionDate: int
    RcCalTransactions: Union[List[GroupedTransaction], List[Transaction]]
    module_id: Optional[PyObjectId] = Field(alias="module_id", default=None)

class ProspectResponse(BaseModel):
    message: str
    result: ProspectModel
class ProspectsResponse(BaseModel):
    message: str
    total: int
    result: List[ProspectModel]