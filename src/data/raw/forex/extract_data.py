from src.utils.spark_loader import get_SparkSession
from src.utils.time_utils import get_unix_time
import subprocess
import json
import argparse
import pyspark.sql.functions as F
import yaml
import time
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import psycopg2
from urllib.parse import urlparse

spark = get_SparkSession()


def preprocess_data(data):
    """
    Convierte los datos al tipo adecuado para evitar conflictos de tipo.
    """
    for record in data:
        record["open"] = float(record["open"])
        record["high"] = float(record["high"])
        record["low"] = float(record["low"])
        record["close"] = float(record["close"])
        record["volume"] = float(record["volume"])
    return data


def fetch_dukascopy_data(symbol, start_date, end_date, timeframe):
    """
    Llama al script de Node.js para descargar datos de Dukascopy.
    """
    try:
        result = subprocess.run(
            [
                "node",
                "../../../../src/node/fetch_dukascopy_data.js",
                symbol,
                start_date,
                end_date,
                timeframe
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        data = json.loads(result.stdout)
        return data
    except subprocess.CalledProcessError as e:
        print("Error al ejecutar el script de Node.js:", e.stderr)
        return None


def insert_data(symbol, start_date, end_date, interval):

    START_TIME, END_TIME, INT_START_TIME, INT_END_TIME = get_unix_time(start_date, end_date)

    print(f"Descargando datos para {symbol} desde {START_TIME} hasta {END_TIME} con intervalo {interval}...")
    data = fetch_dukascopy_data(symbol, START_TIME, END_TIME, interval)

    if not data:
        print("No se encontraron datos.")
        return

    data = preprocess_data(data)

    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
    ])

    df = spark.createDataFrame(data, schema)

    df = df.withColumn("fecha_hora_apertura_dt", F.from_unixtime(df["timestamp"] / 1000).cast("timestamp"))

    df = (
        df
        .withColumnRenamed("open", "monto_apertura_val")
        .withColumnRenamed("high", "monto_maximo_val")
        .withColumnRenamed("low", "monto_minimo_val")
        .withColumnRenamed("close", "monto_cierre_val")
        .withColumn("par_cd", F.lit(symbol))
        .withColumn("intervalo_cd", F.lit(interval))
        .drop("timestamp")
    )

    with open("../../../../conf/local/database_config.yaml", 'r') as stream:
        config = yaml.safe_load(stream)

    jdbc_url = config['database']['url']

    parsed_url = urlparse(jdbc_url.replace("jdbc:", ""))
    host = parsed_url.hostname
    database = parsed_url.path[1:]
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=config['database']['user'],
        password=config['database']['password']
    )
    cursor = conn.cursor()

    delete_query = f"""
        DELETE FROM public.dukascopy_data
        WHERE fecha_hora_apertura_dt BETWEEN '{START_TIME}' AND '{END_TIME}'
          AND par_cd = '{symbol}' AND intervalo_cd = '{interval}'
        """

    try:
        try:
            cursor.execute(delete_query)
            conn.commit()
            print(
                f"Registros eliminados para el rango de fechas y "
                f"Par Activos {symbol} con frecuencia {interval}"
            )
            cursor.close()
            conn.close()

        except Exception as e:
            print(f"Error al eliminar registros: {e}")
            raise

        try:
            df.write.mode("append").format("jdbc").option("url", jdbc_url).option(
                "dbtable", "public.dukascopy_data"
            ).option("user", config["database"]["user"]).option(
                "password", config["database"]["password"]
            ).option(
                "driver", config["database"]["driver"]
            ).save()

            print(
                f"Registros insertados: {df.count()} en la tabla "
                f"binance_data para el rango de fechas y "
                f"Par Activos {symbol} con frecuencia {interval}"
            )
        except Exception as e:
            print(f"Error al insertar registros: {e}")

    except Exception as main_exception:
        print(f"Se produjo un error en el proceso: {main_exception}")


def main(symbol, interval, start_time, end_time):
    print("------------------------------------------------------------------------------------------------------")
    hora_inicio = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print(f"Hora de inicio: {hora_inicio}")

    insert_data(symbol, start_time, end_time, interval)

    tiempo_transcurrido = round(time.time() - time.mktime(time.strptime(hora_inicio, '%Y-%m-%d %H:%M:%S')), 2)
    print(f"Tiempo transcurrido: {tiempo_transcurrido} segundos")

    if interval != "1m":
        for i in range(60, 0, -1):
            print(f"Esperando {i} segundos para la siguiente extracción...", end="\r")
            time.sleep(1)
    else:
        for i in range(30, 0, -1):
            print(f"Esperando {i} segundos para la siguiente extracción...", end="\r")
            time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extracción de datos históricos de Dukascopy.")
    parser.add_argument("--symbol", required=True, help="Símbolo del par de divisas (ej. EURUSD)")
    parser.add_argument("--interval", required=True, help="Intervalo de tiempo (ej. m1, m5)")
    parser.add_argument("--start_time", required=False, default=None, help="Tiempo de inicio (formato: YYYY-MM-DD)")
    parser.add_argument("--end_time", required=False, default=None, help="Tiempo de fin (formato: YYYY-MM-DD)")

    args = parser.parse_args()

    try:
        while True:
            main(args.symbol, args.interval, args.start_time, args.end_time)
    except KeyboardInterrupt:
        print("Proceso interrumpido por el usuario.")
