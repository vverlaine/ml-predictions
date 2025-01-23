from pyspark.sql import SparkSession
from datetime import datetime


def get_SparkSession():

    spark = SparkSession.builder \
        .appName("data-bitcoin") \
        .config("spark.jars", "../../../../conf/jars/postgresql-42.7.3.jar") \
        .config('spark.driver.extraClassPath', 'conf/jars/postgresql-42.7.3.jar') \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.rpc.message.maxSize", "512") \
        .getOrCreate()
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Spark correctamente")

    return spark
