import os
import subprocess
import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import clean_job as clean_main
from src.fr.hymaia.exo2.spark_aggregate_job import aggregate_job as aggregate_main


class TestIntegrationJobs(unittest.TestCase):
    def run_spark_job(self, script_path):
        result = subprocess.run(["spark-submit", script_path], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, f"Le script Spark {script_path} a échoué avec l'erreur : {result.stderr}")

    def test_integration_exo2(self):
        self.run_spark_job("src/fr/hymaia/exo2/spark_clean_job.py")
        self.assertTrue(os.path.exists("data/exo2/output.parquet"), "Le fichier parquet de sortie du job 1 n'a pas été créé.")
        self.run_spark_job("src/fr/hymaia/exo2/spark_aggregate_job.py")
        self.assertTrue(os.path.exists("data/exo2/aggregate.csv"), "Le fichier CSV de sortie du job 2 n'a pas été créé.")


