from typing import List

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StringType
from data_analyser.dataframe_models import TableData


def run_merge_operation(tables: List[TableData]) -> DataFrame:
    """
    Function merges two DataFrames, performs required transformations
    and column operations, and saves the merged DataFrame
    to a CSV file in S3 bucket. It returns the merged DataFrame.
    """
    vgsale_table = [table for table in tables if table.config.name == "vgsales"][0]
    steam_table = [table for table in tables if table.config.name == "steam"][0]
    steam_agg_table_df = steam_table.df.groupBy("game_name").agg(
        F.sum(
            F.when(F.col("operation") == "purchase", F.col("value")).otherwise(0)
        ).alias("global_sales"),
        F.sum(F.when(F.col("operation") == "play", F.col("value")).otherwise(0)).alias(
            "total_play_hours"
        ),
        F.countDistinct("user_id").alias("unique_player_on_steam"),
        F.lit("steam").alias("platform"),
        F.col("game_name").alias("name"),
    )
    steam_agg_table_df = steam_agg_table_df.withColumn(
        "global_sales",
        (F.col("global_sales") / 1000000).cast("double"),
    )
    vgsale_table_metadata = vgsale_table.df.select(
        *[
            col.name
            for col in vgsale_table.df.schema
            if isinstance(col.dataType, StringType)
        ]
    ).distinct()
    steam_agg_table_df = (
        steam_agg_table_df.join(
            vgsale_table_metadata,
            steam_agg_table_df["name"] == vgsale_table_metadata["name"],
            "left",
        )
        .drop(vgsale_table_metadata["name"])
        .drop(vgsale_table_metadata["platform"])
        .drop(vgsale_table_metadata["year"])
    )
    # Find extra columns in the left DataFrame
    extra_columns = set(vgsale_table.df.columns) - set(steam_agg_table_df.columns)

    # Add null columns to the right DataFrame for the extra columns
    for extra_column in extra_columns:
        steam_agg_table_df = steam_agg_table_df.withColumn(extra_column, F.lit(None))
    steam_play_data_table = steam_agg_table_df.groupBy("name").agg(
        F.sum("total_play_hours").alias("total_play_hours"),
        F.sum("unique_player_on_steam").alias("unique_player_on_steam"),
    )
    columns_to_drop = set(steam_agg_table_df.columns) - set(vgsale_table.df.columns)
    for column in columns_to_drop:
        steam_agg_table_df = steam_agg_table_df.drop(column)
    merged_df = vgsale_table.df.unionByName(steam_agg_table_df)
    merged_df = merged_df.join(
        steam_play_data_table,
        merged_df["name"] == steam_play_data_table["name"],
        "left",
    ).drop(steam_play_data_table["name"])
    merged_df = merged_df.coalesce(1)
    merged_df.write.mode("overwrite").csv(
        "s3a://aishwary-test-bucket/indigg-assignment-bucket/output/merged_df",
        header=True,
    )
    print(
        "Merged Data Saved at s3://aishwary-test-bucket/indigg-assignment-bucket/output/merged_df"
    )
    return merged_df
