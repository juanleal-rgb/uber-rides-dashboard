from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.sql import func
from database import Base


class CallRecord(Base):
    __tablename__ = "call_records"

    id         = Column(Integer, primary_key=True, index=True)
    phone      = Column(String(50),  nullable=False, index=True)
    status     = Column(String(50),  nullable=False, default="neutral", index=True)
    sentiment  = Column(String(50),  nullable=False, default="neutral", index=True)
    call_human = Column(Boolean,     nullable=False, default=False)
    summary    = Column(Text)
    attempt    = Column(Integer,     nullable=False, default=1)
    duration   = Column(Integer,     nullable=False, default=0)
    country    = Column(String(2),   nullable=False, default="PT", index=True)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True
    )
