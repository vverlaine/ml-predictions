# COMMAND ----------
from src.utils.spark_loader import get_SparkSession
import yaml
from pyspark.sql import DataFrame
from tensorflow.keras.models import load_model
import pandas as pd
import pyspark.sql.functions as F
from urllib.parse import urlparse
import psycopg2

spark = get_SparkSession()

time_steps = 60
# COMMAND ----------
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


def load_data_forex() -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.data_modelo_forex")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter(F.col("fecha_hora_apertura_dt") < "2025-01-14 12:15:00")
        .orderBy("fecha_hora_apertura_dt")
    )
    return df


# COMMAND ----------
df_processed = load_data_forex().toPandas()

features_used_during_training = df_processed.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).columns
model = load_model("lstm_model_60.keras")

# COMMAND ----------
latest_data = df_processed[features_used_during_training].iloc[-time_steps:].values
latest_data = latest_data.reshape(1, time_steps, -1)
# COMMAND ----------
future_predictions = []
current_input = latest_data.copy()

for _ in range(10):

    next_point = model.predict(current_input)
    future_predictions.append(next_point[0, 0])

    next_input = current_input[:, 1:, :].copy()
    next_input[:, :, -1] = next_point
    current_input = next_input

# Generar las fechas correspondientes para las predicciones
last_date = pd.to_datetime(df_processed["fecha_hora_apertura_dt"].iloc[-1])
future_dates = [last_date + pd.Timedelta(minutes=5 * i) for i in range(1, 11)]

# COMMAND ----------
predictions_df = pd.DataFrame({
    "fecha_hora_apertura_dt": future_dates,
    "prediccion": future_predictions
})

min_date = predictions_df["fecha_hora_apertura_dt"].min()
max_date = predictions_df["fecha_hora_apertura_dt"].max()

print(predictions_df)
print(f"Fecha mínima: {min_date}, Fecha máxima: {max_date}")
# Opcional: Guardar las predicciones en un archivo CSV o en la base de datos
predictions_df.to_csv("future_predictions.csv", index=False)
# COMMAND ----------
delete_query = f"""
    DELETE FROM public.predicciones_futuras_2
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
# Guardar en Postgres
spark.createDataFrame(predictions_df).withColumn(
    "prediccion", F.round(F.col("prediccion"), 8)
).write.format("jdbc").option("url", jdbc_url).option(
    "dbtable", "public.predicciones_futuras_2"
).option(
    "user", config["database"]["user"]
).option(
    "password", config["database"]["password"]
).option(
    "driver", config["database"]["driver"]
).mode(
    "append"
).option(
    "replacewhere", f"fecha_hora_apertura_dt >= '{min_date}' AND fecha_hora_apertura_dt <= '{max_date}'"
).save()

# COMMAND ----------
