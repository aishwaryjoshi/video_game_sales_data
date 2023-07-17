from typing import List, Literal, Optional, Dict
from pydantic import BaseModel


class Glue(BaseModel):
    database: str
    table_names: List[str]
    role: str


class Cleaning(BaseModel):
    handle_nulls: Literal["use_zero", "drop_row"]


class Table(BaseModel):
    name: str
    autodetect: bool
    columns: Optional[List[str]] = None
    types: Optional[List[Literal["str", "int", "double"]]] = None


class Config(BaseModel):
    glue: Glue
    cleaning: Cleaning
    tables: List[Table]
