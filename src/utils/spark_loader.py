from pyspark.sql import SparkSession
from datetime import datetime


def get_SparkSession():

    spark = SparkSession.builder \
        .appName("data-bitcoin") \
        .config("spark.jars", "../../../../conf/jars/postgresql-42.7.3.jar") \
        .config('spark.driver.extraClassPath', 'conf/jars/postgresql-42.7.3.jar') \
        .getOrCreate()
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Spark correctamente")

    return spark
