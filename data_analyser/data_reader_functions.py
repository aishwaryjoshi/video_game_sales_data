from typing import List

import boto3
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    NumericType,
    StringType,
    StructField,
    IntegerType,
    DoubleType,
    StructType,
)
from data_analyser.config_object import Config, Cleaning, Table
from data_analyser.dataframe_models import TableData


def _create_dataframe_schema(table_config: Table) -> StructType:
    fields = []
    if table_config.columns and table_config.types:
        for column, data_type in zip(table_config.columns, table_config.types):
            if data_type == "str":
                fields.append(StructField(column, StringType(), nullable=True))
            elif data_type == "int":
                fields.append(StructField(column, IntegerType(), nullable=True))
            elif data_type == "double":
                fields.append(StructField(column, DoubleType(), nullable=True))
    print(fields)
    return StructType(fields)


def read_from_glue_table(
    spark_session: SparkSession, config: Config
) -> List[TableData]:
    database = config.glue.database
    tables = config.glue.table_names
    glue = boto3.client("glue", region_name="us-east-1")

    for i, table in enumerate(tables):
        glue_catalog_table = glue.get_table(DatabaseName=database, Name=table)
        table_location = glue_catalog_table["Table"]["StorageDescriptor"]["Location"]
        table_location = "s3a" + table_location[2:]
        if config.tables[i].autodetect:
            glue_table_schema = glue_catalog_table["Table"]["StorageDescriptor"][
                "Columns"
            ]
            spark_schema = StructType(
                [
                    StructField(
                        column["Name"],
                        StringType()
                        if column["Type"] == "string"
                        else (
                            DoubleType()
                            if column["Type"] == "double"
                            else IntegerType()
                        ),
                        True,
                    )
                    for column in glue_table_schema
                ]
            )
            df = (
                spark_session.read.format("csv")
                .schema(spark_schema)
                .option("header", "true")
                .load(table_location)
            )
        else:
            schema = _create_dataframe_schema(table_config=config.tables[i])
            df = (
                spark_session.read.format("csv")
                .option("header", "false")
                .schema(schema)
                .load(table_location)
            )
        table_data = TableData(config=config.tables[i], df=df)
        yield table_data


def _deduplicate_data(df: DataFrame) -> DataFrame:
    df = df.distinct()
    return df


def _handle_null_values(df: DataFrame, handle_null_config: str) -> DataFrame:
    column_types = {col_name: col_dataType for col_name, col_dataType in df.dtypes}
    if handle_null_config == "use_zero":
        for column in df.columns:
            if column_types[column] in ["integer", "int", "double", "float", "decimal"]:
                df = df.withColumn(
                    column, F.when(df[column].isNull(), 0).otherwise(df[column])
                )
            if column_types[column] in ["string"]:
                df = df.withColumn(
                    column, F.when(df[column].isNull(), "").otherwise(df[column])
                )
    if handle_null_config == "drop_row":
        for column in df.columns:
            df = df.filter(df[column].isNotNull())
    return df


def clean_data(df: DataFrame, cleaning: Cleaning) -> DataFrame:
    df = _deduplicate_data(df)
    df = _handle_null_values(df, cleaning.handle_nulls)
    return df
