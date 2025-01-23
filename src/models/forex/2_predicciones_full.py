# COMMAND ----------
# Importaciones necesarias
from src.utils.spark_loader import get_SparkSession
import yaml
from pyspark.sql import DataFrame
from tensorflow.keras.models import load_model
import pandas as pd
import pyspark.sql.functions as F
from urllib.parse import urlparse
import psycopg2
from src.data.silver.forex.lags_features_forex_full import build_features_forex, create_lag_features
from src.data.silver.forex.mdt_forex_full import data_trasnform

spark = get_SparkSession()

time_steps = 20
intervalo = "m5"
par_cd = "eurusd"
datetime = "2025-01-14 09:00:00"
stardate = "2024-06-01 00:00:00"
points = 13
# COMMAND ----------
# Leer configuración de base de datos
with open("conf/local/database_config.yaml", 'r') as stream:
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

model = load_model(f"lstm_model_{time_steps}_{intervalo}_{par_cd}.keras")


# Cargar datos
def load_data_raw(intervalo: str, divisa: str) -> DataFrame:
    df_raw = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "forex.raw_forex_dukascopy")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter(F.col("fecha_hora_apertura_dt") >= stardate)
        .filter(F.col("fecha_hora_apertura_dt") < datetime)
        .filter(f"intervalo_cd = '{intervalo}'")
        .filter(f"par_cd = '{divisa}'")
        .select("fecha_hora_apertura_dt", "monto_cierre_val", "monto_minimo_val", "monto_maximo_val")
        .orderBy("fecha_hora_apertura_dt")
    )
    return df_raw


def load_data_lags_features(intervalo: str, divisa: str) -> DataFrame:
    df_raw = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "forex.features_lags_forex_full")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter(F.col("fecha_hora_apertura_dt") >= stardate)
        .filter(F.col("fecha_hora_apertura_dt") < datetime)
        .filter(f"intervalo_cd = '{intervalo}'")
        .filter(f"par_cd = '{divisa}'")
        .orderBy("fecha_hora_apertura_dt")
    )
    return df_raw


def load_data_mdt(intervalo: str, divisa: str) -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "forex.mdt_forex_full")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter(F.col("fecha_hora_apertura_dt") >= stardate)
        .filter(F.col("fecha_hora_apertura_dt") < datetime)
        .filter(f"intervalo_cd = '{intervalo}'")
        .filter(f"par_cd = '{divisa}'")
        .drop("intervalo_cd", "par_cd")
        .orderBy("fecha_hora_apertura_dt")
    )
    return df


def max_date_data(df: DataFrame):
    max_date = df.agg({"fecha_hora_apertura_dt": "max"}).collect()[0][0]
    print("Fecha máxima de los datos: ", max_date)
    return max_date

# COMMAND ----------


# Preparar las predicciones
df_raw = load_data_raw(intervalo, par_cd)
df_lags_features = load_data_lags_features(intervalo, par_cd)
df_mdt = load_data_mdt(intervalo, par_cd)

df_raw_pd = df_raw.toPandas()
df_lags_features_pd = df_lags_features.toPandas()

max_date = max_date_data(df_mdt)
df_mdt_pd = df_mdt.toPandas()

X = df_mdt_pd.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]).columns
ultimos_steps = df_mdt_pd[X].iloc[-time_steps:].values
ultimos_steps = ultimos_steps.reshape(1, time_steps, -1)

# COMMAND ----------
future_predictions = {"fecha_hora_apertura_dt": []}
variables = ["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]

for variable in variables:
    future_predictions[f"prediccion_{variable}"] = []

current_input = ultimos_steps.copy()

for _ in range(points):
    if _ == 0:
        print(f" ************************* Predicción {_ + 1} - { max_date + pd.Timedelta(minutes=5 * (_ + 1))} *************************")

        # Realizar predicción con el modelo
        next_point = model.predict(current_input)

        # Almacenar predicciones para cada variable
        for i, variable in enumerate(variables):
            future_predictions[f"prediccion_{variable}"].append(next_point[0, i])

        # Generar la fecha para la predicción
        date_next_point = max_date + pd.Timedelta(minutes=5)
        future_predictions["fecha_hora_apertura_dt"].append(date_next_point)

        # Actualizar el DataFrame con la nueva predicción
        predictions_df = pd.DataFrame({
            "fecha_hora_apertura_dt": [date_next_point],
            "monto_cierre_val": [next_point[0, 0]],  # Valor predicho para cierre
            "monto_minimo_val": [next_point[0, 1]],  # Valor predicho para mínimo
            "monto_maximo_val": [next_point[0, 2]],  # Valor predicho para máximo
        })

        # Agregar la predicción al DataFrame raw
        df_raw_pd = pd.concat([df_raw_pd, predictions_df], ignore_index=True)

        # Convertir a Spark DataFrame para calcular nuevas características
        df_raw_new = spark.createDataFrame(df_raw_pd).orderBy("fecha_hora_apertura_dt")

        # Calcular características y lags
        df_features = build_features_forex(spark, df_raw_new)
        df_lags_features = create_lag_features(df_features, intervalo, par_cd)
        df_lags_features = df_lags_features.na.drop()
        # Pasar por el proceso de escalado y PCA
        df_lags_features_pd = df_lags_features.toPandas()
        df_mdt = data_trasnform(df_lags_features_pd, False)

    else:
        # Obtener nuevos datos de entrada para la siguiente predicción
        max_date = max_date_data(df_mdt)
        print(f" ************************* Predicción {_ + 1} - {max_date} *************************")
        df_mdt_pd = df_mdt.toPandas()

        X = df_mdt_pd.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]).columns
        ultimos_steps = df_mdt_pd[X].iloc[-time_steps:].values
        ultimos_steps = ultimos_steps.reshape(1, time_steps, -1)
        current_input = ultimos_steps.copy()

        next_point = model.predict(current_input)

        # Almacenar predicciones para cada variable
        for i, variable in enumerate(variables):
            future_predictions[f"prediccion_{variable}"].append(next_point[0, i])

        # Generar la fecha para la predicción
        date_next_point = max_date + pd.Timedelta(minutes=5)
        future_predictions["fecha_hora_apertura_dt"].append(date_next_point)

        # Actualizar el DataFrame con la nueva predicción
        predictions_df = pd.DataFrame({
            "fecha_hora_apertura_dt": [date_next_point],
            "monto_cierre_val": [next_point[0, 0]],  # Valor predicho para cierre
            "monto_minimo_val": [next_point[0, 1]],  # Valor predicho para mínimo
            "monto_maximo_val": [next_point[0, 2]],  # Valor predicho para máximo
        })

        # Agregar la predicción al DataFrame raw
        df_raw_pd = pd.concat([df_raw_pd, predictions_df], ignore_index=True)

        # Convertir a Spark DataFrame para calcular nuevas características
        df_raw_new = spark.createDataFrame(df_raw_pd).orderBy("fecha_hora_apertura_dt")

        # Calcular características y lags
        df_features = build_features_forex(spark, df_raw_new)
        df_lags_features = create_lag_features(df_features, intervalo, par_cd)
        df_lags_features = df_lags_features.na.drop()
        # Pasar por el proceso de escalado y PCA
        df_lags_features_pd = df_lags_features.toPandas()
        df_mdt = data_trasnform(df_lags_features_pd, False)


# COMMAND ----------
# Crear DataFrame con predicciones futuras
df_predictions = (
    df_mdt.filter(F.col("fecha_hora_apertura_dt") >= datetime)
    .select(
        F.col("fecha_hora_apertura_dt"),
        F.col("monto_cierre_val").alias("monto_cierre_val_pred"),
        F.col("monto_minimo_val").alias("monto_minimo_val_pred"),
        F.col("monto_maximo_val").alias("monto_maximo_val_pred"),
    )
)


min_date = df_predictions.agg({"fecha_hora_apertura_dt": "min"}).collect()[0][0]
max_date = df_predictions.agg({"fecha_hora_apertura_dt": "max"}).collect()[0][0]

print(f"Fecha mínima: {min_date}, Fecha máxima: {max_date}")

# COMMAND ----------
# Guardar en Postgres
delete_query = f"""
    DELETE FROM forex.predicciones_full
    WHERE fecha_hora_apertura_dt BETWEEN '{min_date}' AND '{max_date}'
    """

try:
    cursor.execute(delete_query)
    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error al eliminar registros: {e}")
    raise

# COMMAND ----------
# Guardar predicciones en Postgres
df_predictions.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "forex.predicciones_full") \
    .option("user", config["database"]["user"]) \
    .option("password", config["database"]["password"]) \
    .option("driver", config["database"]["driver"]) \
    .mode("append") \
    .save()

# COMMAND ----------
