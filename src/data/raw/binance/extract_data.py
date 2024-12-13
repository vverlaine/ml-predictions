from src.utils.spark_loader import get_SparkSession
from src.utils.time_utils import get_unix_time
from src.data.raw.binance.utils_extract import get_all_historical_klines
import pyspark.sql.functions as F
import yaml
import time
import argparse
import psycopg2
from urllib.parse import urlparse

# START_TIME = None
# END_TIME = None

spark = get_SparkSession()


def insert_data(symbol: str, interval: str, start_time: str, end_time: str):
    START_TIME, END_TIME, INT_START_TIME, INT_END_TIME = get_unix_time(start_time, end_time)
    print(f"Extrayendo datos para el rango de fechas {START_TIME} - {END_TIME}")
    df_binance = get_all_historical_klines(symbol, interval, INT_START_TIME, INT_END_TIME)

    df = spark.createDataFrame(df_binance).drop("ignore")

    df = (
        df
        .withColumnRenamed("timestamp", "fecha_hora_apertura_dt")
        .withColumnRenamed("close_time", "fecha_hora_cierre_dt")
        .withColumnRenamed("open", "monto_apertura_val")
        .withColumnRenamed("close", "monto_cierre_val")
        .withColumnRenamed("high", "monto_maximo_val")
        .withColumnRenamed("low", "monto_minimo_val")
        .withColumnRenamed("volume", "volumen_criptomoneda_negociado_val")
        .withColumnRenamed("quote_asset_volume", "volumen_moneda_negociado_val")
        .withColumnRenamed("number_of_trades", "cantidad_operaciones_val")
        .withColumnRenamed("taker_buy_base_asset_volume", "volumen_criptomoneda_comprado_val")
        .withColumnRenamed("taker_buy_quote_asset_volume", "volumen_moneda_comprado_val")
        .withColumn("par_cd", F.lit(symbol))
        .withColumn("intervalo_cd", F.lit(interval))
        .withColumn("fecha_hora_apertura_dt", F.to_timestamp("fecha_hora_apertura_dt", "yyyy-MM-dd HH:mm"))
        .withColumn("fecha_hora_cierre_dt", F.to_timestamp("fecha_hora_cierre_dt", "yyyy-MM-dd HH:mm"))
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
        DELETE FROM public.binance_data
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
                "dbtable", "public.binance_data"
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

    insert_data(symbol, interval, start_time, end_time)

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
    parser = argparse.ArgumentParser(description="Extracción de datos históricos de Binance.")
    parser.add_argument("--symbol", required=True, help="Símbolo de la criptomoneda (ej. XRPUSDT)")
    parser.add_argument("--interval", required=True, help="Intervalo de tiempo (ej. 1m, 5m)")
    parser.add_argument("--start_time", required=False, default=None, help="Tiempo de inicio (formato: YYYY-MM-DD HH:mm)")
    parser.add_argument("--end_time", required=False, default=None, help="Tiempo de fin (formato: YYYY-MM-DD HH:mm)")

    args = parser.parse_args()
    try:
        while True:
            main(args.symbol, args.interval, args.start_time, args.end_time)
    except KeyboardInterrupt:
        print("Proceso interrumpido por el usuario")
