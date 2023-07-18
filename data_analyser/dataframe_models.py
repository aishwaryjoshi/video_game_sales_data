from dataclasses import dataclass
from data_analyser.config_object import Table
from pyspark.sql import DataFrame


@dataclass
class TableData:
    """
    Define a data class TableData using the dataclass decorator. The class has two attributes:
    config: It is of type Table and represents the configuration of a table.
    df: It is of type DataFrame and represents the data stored in the table as a DataFrame.
    """

    config: Table
    df: DataFrame
