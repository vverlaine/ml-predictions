from src.utils.spark_loader import get_SparkSession
import pyspark.sql.functions as F
import yaml
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import ta

spark = get_SparkSession()

with open("conf/local/database_config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

jdbc_url = config['database']['url']

INTERVALS = {
    1: 5,
    2: 10,
    3: 15,
    4: 20,
    5: 25,
    6: 30,
    7: 35,
    8: 40,
    9: 45,
    10: 50,
    11: 55,
    12: 60,
    13: 70,
    14: 80,
    15: 90,
    16: 100,
    17: 120,
    18: 150,
    19: 180,
    20: 210,
    21: 240,
    22: 270,
    23: 300,
    24: 330,
    25: 360,
}

COLUMNS = [
    "monto_apertura_val",
    "monto_cierre_val",
    "monto_maximo_val",
    "monto_minimo_val",
    "volumen_criptomoneda_negociado_val",
    "volumen_moneda_negociado_val",
    "volumen_criptomoneda_comprado_val",
    "volumen_moneda_comprado_val"
]


def extract_data(INTERVAL: str) -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.binance_data")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter(F.col("intervalo_cd") == INTERVAL)
    )
    return df


def create_lag_features(df: DataFrame) -> DataFrame:
    for column in COLUMNS:
        for interval, suffix in INTERVALS.items():
            window = Window.orderBy("fecha_hora_apertura_dt")
            df = df.withColumn(
                f"{column}_{suffix}m",
                F.lag(F.col(column), interval).over(window)
            )
    return df


def create_dataframe(INTERVAL) -> DataFrame:
    df_pd = (
        extract_data(INTERVAL)
        .drop("fecha_hora_cierre_dt", "par_cd", "intervalo_cd", "cantidad_operaciones_val")
        .toPandas()
    )

    df_pd["sma_50"] = ta.trend.sma_indicator(df_pd["monto_cierre_val"], window=50)
    df_pd["ema_50"] = ta.trend.ema_indicator(df_pd["monto_cierre_val"], window=50)
    df_pd["rsi"] = ta.momentum.rsi(df_pd["monto_cierre_val"], window=14)
    df_pd["macd"] = ta.trend.macd(df_pd["monto_cierre_val"])
    df_pd['Signal_Line'] = ta.trend.macd_signal(df_pd['monto_cierre_val'])
    df_pd['Histogram'] = df_pd['macd'] - df_pd['Signal_Line']
    bb_indicator = ta.volatility.BollingerBands(df_pd["monto_cierre_val"])
    df_pd["bb_bbm"], df_pd["bb_bbh"], df_pd["bb_bbl"] = (
        bb_indicator.bollinger_mavg(),
        bb_indicator.bollinger_hband(),
        bb_indicator.bollinger_lband(),
    )

    df = spark.createDataFrame(df_pd)

    df_lagged = (
        create_lag_features(df)
        .filter(F.col("fecha_hora_apertura_dt") >= "2023-01-01 00:00")
        .withColumn(
            "cambio_pct",
            (F.col("monto_cierre_val") - F.col("monto_apertura_val"))
            / F.col("monto_apertura_val"),
        )
        .withColumn(
            "diferencia_relativa",
            (F.col("monto_maximo_val") - F.col("monto_minimo_val"))
            / F.col("monto_apertura_val"),
        )
        .withColumn(
            "retorno_logaritmico",
            F.log(F.col("monto_cierre_val") / F.col("monto_cierre_val_5m")),
        )
    )

    return df_lagged


df_lagged = create_dataframe("5m")

# Filtar df_lagged con la ultima fecha_hora_apertura_dt, se debe calcular cual es la ultima fecha_hora_apertura_dt
# para cada intervalo y luego filtrar el df_lagged con esas fechas
df_lagged_lastest = df_lagged.filter(
    F.col("fecha_hora_apertura_dt")
    == df_lagged.agg({"fecha_hora_apertura_dt": "max"}).collect()[0][0]
)

# df_lagged_lastest.show()


df_lagged.write.mode("overwrite").format("jdbc").option("url", jdbc_url).option(
    "dbtable", "public.mdt_binance"
).option("user", config["database"]["user"]).option(
    "password", config["database"]["password"]
).option(
    "driver", config["database"]["driver"]
).save()
