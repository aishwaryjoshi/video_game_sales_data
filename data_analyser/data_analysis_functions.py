from typing import List
import os

from pyspark.sql import functions as F, DataFrame
import matplotlib.pyplot as plt
import pandas as pd
from data_analyser.dataframe_models import TableData


def _sales_per_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Function performs aggregation on the input DataFrame
    to calculate the total sales per column. Then plot
    the results using Matplotlib and saves the plot
    as a PDF file in the "plotted_graphs" directory.
    Only top 20 values will be shown. Rest will be put in others bucket.
    Null values will be in unknown bucket.
    """
    # Aggregate the sales columns using the sum function
    sales_aggregated = df.groupBy(column_name).agg(
        F.sum("global_sales").alias("total_global_sales"),
    )
    top_20_column_values = (
        sales_aggregated.orderBy(F.col("total_global_sales").desc())
        .select(column_name)
        .limit(20)
        .collect()
    )
    top_20_column_values_str = []
    for column_value in top_20_column_values:
        top_20_column_values_str.append(column_value.asDict()[column_name])
    sales_aggregated = sales_aggregated.withColumn(
        column_name,
        F.when(
            F.col(column_name).isin(top_20_column_values_str), F.col(column_name)
        ).otherwise(F.lit(f"Other {column_name}")),
    )
    sales_aggregated = sales_aggregated.groupBy(column_name).agg(
        F.sum("total_global_sales").alias("total_global_sales"),
    )
    sales_aggregated = sales_aggregated.withColumn(
        column_name,
        F.when(
            sales_aggregated[column_name].isNull(), F.lit(f"Unknown {column_name}")
        ).otherwise(sales_aggregated[column_name]),
    )
    # Convert the aggregated data to Pandas DataFrame
    sales_pandas = sales_aggregated.toPandas()
    plt.figure(figsize=(60, 10))
    plt.bar(sales_pandas[column_name], sales_pandas["total_global_sales"])
    plt.xlabel(column_name)
    plt.ylabel("Total Sales")
    plt.title(f"Total Sales by {column_name}")
    save_dir = "./plotted_graphs"
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(f"{save_dir}/sales_per_{column_name}.pdf")


def _scatter_plot_for_sales(df: DataFrame, column_name: str):
    df = df.filter(df[column_name].isNotNull())
    df = df.filter(df["global_sales"].isNotNull())
    df = (
        df.groupBy("name")
        .agg(
            F.sum("global_sales").alias("total_sales"),
            F.max(F.col(column_name)).alias(column_name),
        )
        .drop("name")
    )
    pd_df = df.toPandas()
    plt.scatter(pd_df["total_sales"], pd_df[column_name])
    plt.xlabel("Total Sales")
    plt.ylabel(column_name)
    plt.title(f"Scatter Chart Total Sales vs {column_name}")
    save_dir = "./plotted_graphs"
    os.makedirs(save_dir, exist_ok=True)
    plt.savefig(f"{save_dir}/scatter_of_total_sales_vs_{column_name}.pdf")


def perform_analysis(df: DataFrame) -> DataFrame:
    """
    Wrapper/helper function to invoke above function, which performs sales analysis
    """
    _sales_per_column(df, "genre")
    _sales_per_column(df, "platform")
    _sales_per_column(df, "publisher")
    _scatter_plot_for_sales(df, "total_play_hours")
    _scatter_plot_for_sales(df, "unique_player_on_steam")
