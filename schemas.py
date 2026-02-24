from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional


class CallRecordCreate(BaseModel):
    """Validates and coerces the incoming POST payload."""
    phone:      str
    status:     str           = "neutral"
    sentiment:  str           = "neutral"
    call_human: bool          = False
    summary:    Optional[str] = None
    attempt:    int           = 1

    @field_validator("call_human", mode="before")
    @classmethod
    def parse_call_human(cls, v) -> bool:
        if isinstance(v, bool):
            return v
        return str(v).strip().upper() == "TRUE"

    @field_validator("attempt", mode="before")
    @classmethod
    def parse_attempt(cls, v) -> int:
        try:
            return int(v)
        except (ValueError, TypeError):
            return 1

    model_config = {"populate_by_name": True}


class CallRecordResponse(BaseModel):
    id:         int
    phone:      str
    status:     str
    sentiment:  str
    call_human: bool
    summary:    Optional[str]
    attempt:    int
    created_at: datetime

    model_config = {"from_attributes": True}
