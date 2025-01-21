import pyspark.sql.functions as F
import yaml
from pyspark.sql import DataFrame
import ta
from pyspark.sql.window import Window
import psycopg2
from urllib.parse import urlparse

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
    18: 140,
    19: 160,
    20: 180,
    21: 200,
    22: 220,
    23: 240,
    24: 260,
    25: 280,
    26: 300,
    27: 320,
    28: 340,
    29: 360,
    30: 380,
    31: 400,
}

COLUMNS = [
    # "monto_apertura_val",
    "monto_cierre_val",
    # "monto_maximo_val",
    # "monto_minimo_val",
    # "volume",
]

# with open("../../../../conf/local/database_config.yaml", 'r') as stream:
with open("conf/local/database_config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

jdbc_url = config['database']['url']


def load_data_forex(spark, intervalo: str, divisa: str) -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "forex.raw_forex_dukascopy")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter(f"intervalo_cd = '{intervalo}'")
        .filter(f"par_cd = '{divisa}'")
    )
    return df


def build_features_forex(spark, df: DataFrame) -> DataFrame:
    df_pd = df.toPandas()

    df_pd["sma_50"] = ta.trend.sma_indicator(df_pd["monto_cierre_val"], window=50)
    df_pd["ema_50"] = ta.trend.ema_indicator(df_pd["monto_cierre_val"], window=50)
    df_pd["rsi_14"] = ta.momentum.rsi(df_pd["monto_cierre_val"], window=14)

    df_pd["sma_30"] = ta.trend.sma_indicator(df_pd["monto_cierre_val"], window=30)
    df_pd["ema_30"] = ta.trend.ema_indicator(df_pd["monto_cierre_val"], window=30)
    df_pd["rsi_9"] = ta.momentum.rsi(df_pd["monto_cierre_val"], window=9)

    df_pd["sma_20"] = ta.trend.sma_indicator(df_pd["monto_cierre_val"], window=20)
    df_pd["ema_20"] = ta.trend.ema_indicator(df_pd["monto_cierre_val"], window=20)
    df_pd["rsi_6"] = ta.momentum.rsi(df_pd["monto_cierre_val"], window=6)

    df_pd["sma_200"] = ta.trend.sma_indicator(df_pd["monto_cierre_val"], window=200)
    df_pd["ema_200"] = ta.trend.ema_indicator(df_pd["monto_cierre_val"], window=200)

    df_pd["macd"] = ta.trend.macd(df_pd["monto_cierre_val"])
    df_pd['Signal_Line'] = ta.trend.macd_signal(df_pd['monto_cierre_val'])
    df_pd['Histogram'] = df_pd['macd'] - df_pd['Signal_Line']
    bb_indicator = ta.volatility.BollingerBands(df_pd["monto_cierre_val"])
    df_pd["bb_bbm"], df_pd["bb_bbh"], df_pd["bb_bbl"] = (
        bb_indicator.bollinger_mavg(),
        bb_indicator.bollinger_hband(),
        bb_indicator.bollinger_lband(),
    )

    # df_pd["atr_14"] = ta.volatility.average_true_range(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     close=df_pd["monto_cierre_val"],
    #     window=14,
    # )

    # kc_indicator = ta.volatility.KeltnerChannel(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     close=df_pd["monto_cierre_val"],
    #     window=20,
    # )
    # df_pd["kc_bbm"], df_pd["kc_bbh"], df_pd["kc_bbl"] = (
    #     kc_indicator.keltner_channel_mband(),
    #     kc_indicator.keltner_channel_hband(),
    #     kc_indicator.keltner_channel_lband(),
    # )

    # stoch = ta.momentum.stoch(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     close=df_pd["monto_cierre_val"],
    #     window=14,
    #     smooth_window=3,
    # )
    # df_pd["stoch_k"] = stoch
    # df_pd["stoch_d"] = ta.momentum.stoch_signal(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     close=df_pd["monto_cierre_val"],
    #     window=14,
    #     smooth_window=3,
    # )

    # df_pd["williams_r"] = ta.momentum.williams_r(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     close=df_pd["monto_cierre_val"],
    #     lbp=14,
    # )

    df_pd["roc_12"] = ta.momentum.roc(df_pd["monto_cierre_val"], window=12)

    # df_pd["adx"] = ta.trend.adx(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     close=df_pd["monto_cierre_val"],
    #     window=14,
    # )

    # df_pd["ichimoku_a"] = ta.trend.ichimoku_a(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     window1=9,  # Línea de conversión
    #     window2=26,  # Línea base
    # )

    # df_pd["ichimoku_b"] = ta.trend.ichimoku_b(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     window2=26,  # Línea base
    #     window3=52,  # Span B
    # )

    # df_pd["ichimoku_base"] = ta.trend.ichimoku_base_line(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     window2=26,  # Línea base
    # )

    # df_pd["ichimoku_conversion"] = ta.trend.ichimoku_conversion_line(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     window1=9,  # Línea de conversión
    # )

    df_pd["obv"] = ta.volume.on_balance_volume(
        close=df_pd["monto_cierre_val"], volume=df_pd["volume"]
    )

    # df_pd["cmf"] = ta.volume.chaikin_money_flow(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     close=df_pd["monto_cierre_val"],
    #     volume=df_pd["volume"],
    #     window=20,
    # )

    # df_pd["pivot"] = (df_pd["monto_maximo_val"] + df_pd["monto_minimo_val"] + df_pd["monto_cierre_val"]) / 3
    # df_pd["r1"] = 2 * df_pd["pivot"] - df_pd["monto_minimo_val"]
    # df_pd["s1"] = 2 * df_pd["pivot"] - df_pd["monto_maximo_val"]
    # df_pd["r2"] = df_pd["pivot"] + (df_pd["monto_maximo_val"] - df_pd["monto_minimo_val"])
    # df_pd["s2"] = df_pd["pivot"] - (df_pd["monto_maximo_val"] - df_pd["monto_minimo_val"])

    # df_pd["vwap"] = ta.volume.volume_weighted_average_price(
    #     high=df_pd["monto_maximo_val"],
    #     low=df_pd["monto_minimo_val"],
    #     close=df_pd["monto_cierre_val"],
    #     volume=df_pd["volume"],
    # )

    df_pd["day_of_week"] = df_pd["fecha_hora_apertura_dt"].dt.dayofweek

    df_pd["hour"] = df_pd["fecha_hora_apertura_dt"].dt.hour

    df = spark.createDataFrame(df_pd)

    return df


def create_lag_features(df: DataFrame, intervalo: str, divisa: str) -> DataFrame:
    for column in COLUMNS:
        for interval, suffix in INTERVALS.items():
            window = Window.orderBy("fecha_hora_apertura_dt")
            df = df.withColumn(f"{column}_{suffix}m", F.lag(F.col(column), interval).over(window))

    df.withColumn("intervalo_cd", F.lit(intervalo)).withColumn("par_cd", F.lit(divisa))
    return df


def delete_data(intervalo: str, divisa: str):
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
        DELETE FROM forex.features_lags_forex
        WHERE par_cd = '{divisa}' AND intervalo_cd = '{intervalo}'
        """

    try:
        cursor.execute(delete_query)
        conn.commit()
        print(
            f"Registros eliminados para el rango de fechas y "
            f"Par Activos {divisa} con frecuencia {intervalo}"
        )
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error al eliminar registros: {e}")
        raise


def save_postgres(df: DataFrame, intervalo: str, divisa: str):
    delete_data(intervalo, divisa)

    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "forex.features_lags_forex") \
        .option("user", config["database"]["user"]) \
        .option("password", config["database"]["password"]) \
        .option("driver", config["database"]["driver"]) \
        .mode("append") \
        .save()


def main(spark, divisa: str, intervalo: str):
    df = load_data_forex(spark, intervalo, divisa)
    df = build_features_forex(spark, df)
    df = create_lag_features(df, intervalo, divisa)
    df = df.na.drop()
    save_postgres(df, intervalo, divisa)
    print("Proceso finalizado de creación de features y lags")
