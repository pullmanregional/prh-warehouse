from sqlalchemy.orm import registry
from sqlmodel import Field, SQLModel, Relationship
from typing import List
from datetime import datetime, date, time


class PrwModel(SQLModel, registry=registry()):
    pass


class PrwMeta(PrwModel, table=True):
    __tablename__ = "prw_meta"
    id: int | None = Field(default=None, primary_key=True)
    modified: datetime


class PrwSourcesMeta(PrwModel, table=True):
    __tablename__ = "prw_sources_meta"
    id: int | None = Field(default=None, primary_key=True)
    filename: str = Field(unique=True)
    modified: datetime


class PrwPatient(PrwModel, table=True):
    __tablename__ = "prw_patients"

    id: int | None = Field(default=None, primary_key=True)
    prw_id: str = Field(
        unique=True, max_length=24, description="ID hash from unique salt and row ID"
    )
    sex: str = Field(regex="^[MFO]$")
    age: int | None = Field(description="Age in years")
    age_mo: int | None = Field(description="Age in months if <2yo")
    city: str | None = None
    state: str | None = None
    pcp: str | None = None

    encounters: List["PrwEncounter"] = Relationship(back_populates="patient")


class PrwEncounter(PrwModel, table=True):
    __tablename__ = "prw_encounters"

    id: int | None = Field(default=None, primary_key=True)
    prw_id: str = Field(foreign_key="prw_patients.prw_id")
    location: str
    dept: str
    encounter_date: date
    encounter_time: time
    encounter_type: str
    service_provider: str | None = None
    billing_provider: str | None = None
    with_pcp: bool | None = None
    appt_status: str | None = None
    diagnoses: str | None = None
    level_of_service: str | None = None

    patient: PrwPatient | None = Relationship(back_populates="encounters")
