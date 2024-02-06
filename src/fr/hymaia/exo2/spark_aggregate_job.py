import os
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.agregate.aggregate_functions import departement_count

def aggregate_job(input_path, output_path):
    with SparkSession.builder.appName("spark_aggregate_job").master("local[*]").getOrCreate() as spark:
        departement_count(spark.read.parquet(input_path)) \
            .write.mode("overwrite").csv(output_path)
def main():
    path = "data/exo2/output.parquet"
    output_path = "data/exo2/aggregate.csv"
    aggregate_job(path, output_path)

