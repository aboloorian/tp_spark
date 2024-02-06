import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("wordcount").master("local[*]").getOrCreate()
    result_df = wordcount(spark.read.csv("src/resources/exo1/data.csv", header=True, inferSchema=True), "text")
    result_df.show()
    result_df.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))).groupBy('word').count()