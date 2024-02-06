import os
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.clean.clean_functions import join_dataframes, add_departement_column
def clean_job(input_city, input_clients, output_path):
    with SparkSession.builder.appName("spark_clean_job").master("local[*]").getOrCreate() as spark:
        city_zipcode_df = spark.read.csv(input_city, header=True, inferSchema=True)
        clients_bdd_df = spark.read.csv(input_clients, header=True, inferSchema=True)
        add_departement_column(join_dataframes(clients_bdd_df, city_zipcode_df)) \
            .write.mode("overwrite").parquet(output_path)
def main():
    city_zipcode = "src/resources/exo2/city_zipcode.csv"
    clients_bdd = "src/resources/exo2/clients_bdd.csv"
    output_path = "data/exo2/output.parquet"
    clean_job(city_zipcode, clients_bdd, output_path)
