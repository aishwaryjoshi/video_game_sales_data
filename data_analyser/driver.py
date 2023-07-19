from pyspark.sql import SparkSession
import yaml
from data_analyser.config_object import Config
from data_analyser.data_reader_functions import read_from_glue_table, clean_data
from data_analyser.data_merger_functions import run_merge_operation
from data_analyser.data_analysis_functions import perform_analysis


def get_config() -> Config:
    """
    function reads configs.yaml file,
    loads its contents, and returns the object
    containing the configuration information from the file.
    """
    with open("configs.yaml", "r") as file:
        config = Config(**yaml.safe_load(file))
        return config


def get_spark_session(app_name: str) -> SparkSession:
    """
    set up SparkSession with required configurations with access and secret keys to S3
    and returns the created SparkSession object.
    It allows you to interact with Apache Spark using the configured session.
    """
    spark_session = (
        SparkSession.builder.config(
            "spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.12:3.4.1"
        )
        .config("spark.hadoop.fs.s3a.access.key", "AKIA2NVFJCO3UFOUJ5JN")
        .config(
            "spark.hadoop.fs.s3a.secret.key", "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"
        )
        .appName(app_name)
        .getOrCreate()
    )
    return spark_session


if __name__ == "__main__":
    config = get_config()
    spark_session = get_spark_session("indigg_assignment")
    tables = []
    for table_data in read_from_glue_table(spark_session, config):
        table_data.df = clean_data(table_data.df, config.cleaning)
        tables.append(table_data)
    merged_df = run_merge_operation(tables)
    perform_analysis(merged_df)
