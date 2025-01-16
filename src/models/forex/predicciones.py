# COMMAND ----------
from src.utils.spark_loader import get_SparkSession
import yaml
from pyspark.sql import DataFrame
from tensorflow.keras.models import load_model
import numpy as np
import pandas as pd

spark = get_SparkSession()

time_steps = 30
# COMMAND ----------
with open("conf/local/database_config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

jdbc_url = config['database']['url']


def load_data_forex() -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.data_modelo_forex")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
    )
    return df


# COMMAND ----------
df_processed = load_data_forex().toPandas()

features_used_during_training = df_processed.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).columns
# Carga el modelo previamente guardado
model = load_model("lstm_model.keras")

# COMMAND ----------
latest_data = df_processed[features_used_during_training].iloc[-time_steps:].values
latest_data = latest_data.reshape(1, time_steps, -1)
# COMMAND ----------
# Generar múltiples predicciones (10 puntos)
future_predictions = []
current_input = latest_data.copy()

for _ in range(10):  # Generar 10 predicciones futuras
    # Realizar la predicción
    next_point = model.predict(current_input)
    future_predictions.append(next_point[0, 0])  # Almacenar la predicción

    # Ajustar el formato de `next_point` para concatenarlo correctamente
    next_point_reshaped = np.expand_dims(next_point, axis=-1)  # Añadir una dimensión adicional
    current_input = np.append(current_input[:, 1:, :], next_point_reshaped, axis=1)

# Generar las fechas correspondientes para las predicciones
last_date = pd.to_datetime(df_processed["fecha_hora_apertura_dt"].iloc[-1])
future_dates = [last_date + pd.Timedelta(minutes=5 * i) for i in range(1, 11)]

# COMMAND ----------
# Crear un DataFrame con las predicciones y las fechas futuras
predictions_df = pd.DataFrame({
    "fecha_hora_apertura_dt": future_dates,
    "prediccion": future_predictions
})

# Mostrar las predicciones
print(predictions_df)

# Opcional: Guardar las predicciones en un archivo CSV o en la base de datos
predictions_df.to_csv("future_predictions.csv", index=False)

# Guardar en Postgres
spark.createDataFrame(predictions_df).write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "public.predicciones_futuras") \
    .option("user", config["database"]["user"]) \
    .option("password", config["database"]["password"]) \
    .option("driver", config["database"]["driver"]) \
    .mode("overwrite") \
    .save()
