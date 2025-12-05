from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, validates
from pydantic import BaseModel
from typing import List, Optional
import random

SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI(title="Lead Distribution Service")

class Operator(Base):
    __tablename__ = "operators"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    max_active_leads = Column(Integer, default=10)

    @validates('max_active_leads')
    def validate_max_active_leads(self, key, value):
        if value < 0:
            raise ValueError("max_active_leads must be >= 0")
        return value

class Lead(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True, nullable=False)

class Source(Base):
    __tablename__ = "sources"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True)

class SourceOperator(Base):
    __tablename__ = "source_operators"
    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(Integer, ForeignKey("sources.id"), nullable=False)
    operator_id = Column(Integer, ForeignKey("operators.id"), nullable=False)
    weight = Column(Integer, default=1)

class Contact(Base):
    __tablename__ = "contacts"
    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, ForeignKey("leads.id"), nullable=False)
    source_id = Column(Integer, ForeignKey("sources.id"), nullable=False)
    operator_id = Column(Integer, ForeignKey("operators.id"), nullable=True)
    created_at = Column(DateTime, default=func.now())
    is_active = Column(Boolean, default=True)

Base.metadata.create_all(bind=engine)

class OperatorCreate(BaseModel):
    name: str
    is_active: bool = True
    max_active_leads: int = 10

class OperatorOut(BaseModel):
    id: int
    name: str
    is_active: bool
    max_active_leads: int
    class Config:
        from_attributes = True

class SourceCreate(BaseModel):
    name: str

class SourceOut(BaseModel):
    id: int
    name: str
    class Config:
        from_attributes = True

class SourceOperatorAssign(BaseModel):
    operator_id: int
    weight: int

class ContactCreate(BaseModel):
    lead_external_id: str
    source_id: int

class ContactResponse(BaseModel):
    contact_id: int
    lead_id: int
    source_id: int
    operator_id: Optional[int]
    assigned: bool

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_or_create_lead(db, external_id: str):
    lead = db.query(Lead).filter(Lead.external_id == external_id).first()
    if not lead:
        lead = Lead(external_id=external_id)
        db.add(lead)
        db.commit()
        db.refresh(lead)
    return lead

def get_operator_load(db, operator_id: int) -> int:
    return db.query(Contact).filter(
        Contact.operator_id == operator_id,
        Contact.is_active == True
    ).count()

def get_eligible_operators(db, source_id: int):
    query = db.query(Operator, SourceOperator.weight).join(
        SourceOperator, Operator.id == SourceOperator.operator_id
    ).filter(
        SourceOperator.source_id == source_id,
        Operator.is_active == True
    )
    candidates = []
    for op, weight in query.all():
        load = get_operator_load(db, op.id)
        if load < op.max_active_leads and weight > 0:
            candidates.append((op, weight))
    return candidates

def weighted_random_choice(candidates):
    if not candidates:
        return None
    operators, weights = zip(*candidates)
    total = sum(weights)
    r = random.uniform(0, total)
    upto = 0
    for op, w in candidates:
        if upto + w >= r:
            return op
        upto += w
    return operators[-1]

@app.post("/operators/", response_model=OperatorOut)
def create_operator(op: OperatorCreate):
    db = SessionLocal()
    try:
        db_op = Operator(
            name=op.name,
            is_active=op.is_active,
            max_active_leads=op.max_active_leads
        )
        db.add(db_op)
        db.commit()
        db.refresh(db_op)
        return db_op
    finally:
        db.close()

@app.get("/operators/", response_model=List[OperatorOut])
def list_operators():
    db = SessionLocal()
    try:
        return db.query(Operator).all()
    finally:
        db.close()

@app.post("/sources/", response_model=SourceOut)
def create_source(src: SourceCreate):
    db = SessionLocal()
    try:
        db_src = Source(name=src.name)
        db.add(db_src)
        db.commit()
        db.refresh(db_src)
        return db_src
    finally:
        db.close()

@app.post("/sources/{source_id}/operators/")
def assign_operator_to_source(source_id: int, assignment: SourceOperatorAssign):
    db = SessionLocal()
    try:
        db_so = SourceOperator(
            source_id=source_id,
            operator_id=assignment.operator_id,
            weight=assignment.weight
        )
        db.add(db_so)
        db.commit()
        return {"status": "ok"}
    finally:
        db.close()

@app.post("/contacts/", response_model=ContactResponse)
def create_contact(contact_: ContactCreate):
    db = SessionLocal()
    try:
        lead = get_or_create_lead(db, contact_data.lead_external_id)
        source = db.query(Source).filter(Source.id == contact_data.source_id).first()
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
        
        candidates = get_eligible_operators(db, contact_data.source_id)
        selected_operator = weighted_random_choice(candidates)

        contact = Contact(
            lead_id=lead.id,
            source_id=contact_data.source_id,
            operator_id=selected_operator.id if selected_operator else None,
            is_active=True
        )
        db.add(contact)
        db.commit()
        db.refresh(contact)

        return ContactResponse(
            contact_id=contact.id,
            lead_id=lead.id,
            source_id=contact.source_id,
            operator_id=contact.operator_id,
            assigned=contact.operator_id is not None
        )
    finally:
        db.close()

@app.get("/stats/")
def stats():
    db = SessionLocal()
    try:
        contacts = db.query(Contact).all()
        return {
            "total_contacts": len(contacts),
            "assigned": len([c for c in contacts if c.operator_id is not None]),
            "unassigned": len([c for c in contacts if c.operator_id is None])
        }
    finally:
        db.close()