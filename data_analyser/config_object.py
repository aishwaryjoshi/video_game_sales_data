from typing import List, Literal, Optional, Dict
from pydantic import BaseModel


class Glue(BaseModel):
    """
    Breakdown of the given class attributes:
    database: It is of type str and represents the name of the database in AWS Glue.
    table_names: It is of type List[str] and represents a list of table names in AWS Glue.
    role: It is of type str and represents the role associated with AWS Glue,
    which determines the access and permissions for executing Glue operations.
    """

    database: str
    table_names: List[str]
    role: str


class Cleaning(BaseModel):
    """
    Given class attributes represent the option for handling null values
    during the cleaning process. The value can be either "use_zero" or "drop_row",
    indicating different strategies for dealing with null values in the data.
    """

    handle_nulls: Literal["use_zero", "drop_row"]


class Table(BaseModel):
    """
    name: Type str & represents the table name.
    autodetect: Type bool & represents whether the table schema should be auto-detected or not.
    columns: Optional column names list for the table. It can specify the explicit columns in the table schema.
    types: Optional column types list for the table. May specify the explicit data types for columns in the table schema.
    """

    name: str
    autodetect: bool
    columns: Optional[List[str]] = None
    types: Optional[List[Literal["str", "int", "double"]]] = None


class Config(BaseModel):
    glue: Glue
    cleaning: Cleaning
    tables: List[Table]
