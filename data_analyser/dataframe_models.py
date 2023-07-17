from dataclasses import dataclass
from data_analyser.config_object import Table
from pyspark.sql import DataFrame


@dataclass
class TableData:
    config: Table
    df: DataFrame
