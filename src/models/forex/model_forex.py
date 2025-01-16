# COMMAND ----------
from src.utils.spark_loader import get_SparkSession
from sklearn.model_selection import train_test_split
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import numpy as np
import matplotlib.pyplot as plt
import yaml
from pyspark.sql import DataFrame
import seaborn as sns
from sklearn.metrics import mean_squared_error, mean_absolute_error


spark = get_SparkSession()
# COMMAND ----------
# with open("../../../../conf/local/database_config.yaml", 'r') as stream:
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


def prepare_lstm_data(df, target_column="monto_cierre_val", time_steps=10):
    data = df.drop(columns=["fecha_hora_apertura_dt"]).values
    target_index = df.columns.get_loc(target_column)
    X, y = [], []

    print("Inicia preparación de datos")
    for i in range(len(data) - time_steps):
        X.append(data[i:i + time_steps, :])
        y.append(data[i + time_steps, target_index])
    print("Preparación terminada")

    return np.array(X), np.array(y)


def build_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.2))
    model.add(LSTM(units=50, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(units=1))

    model.compile(optimizer="adam", loss="mean_squared_error")
    return model


def train_and_predict_lstm(df_final, time_steps=10):
    print("Prepara los datos")
    X, y = prepare_lstm_data(df_final, time_steps=time_steps)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("Construye el modelo")
    model = build_lstm_model(input_shape=(X_train.shape[1], X_train.shape[2]))
    print(model.summary())

    print("Entrena el modelo")
    model.fit(X_train, y_train, epochs=50, batch_size=32, validation_split=0.2)

    # Predicciones
    y_pred = model.predict(X_test)
    return y_test, y_pred


# COMMAND ----------
df_processed = load_data_forex().toPandas()
y_test, y_pred = train_and_predict_lstm(df_processed)

# COMMAND ----------
plt.figure(figsize=(12, 6))
plt.plot(y_test[:50], label="Valores reales")
plt.plot(y_pred[:50], label="Predicciones")
plt.legend()
plt.title("Predicción con LSTM")
plt.show()
# COMMAND ----------
errores = y_test - y_pred
plt.figure(figsize=(12, 6))
sns.histplot(errores, kde=True, bins=50, label="Distribución de errores")
plt.axvline(0, color="red", linestyle="--", label="Error medio")
plt.legend()
plt.title("Distribución de los errores de predicción")
plt.show()
# COMMAND ----------


rmse = np.sqrt(mean_squared_error(y_test, y_pred))
mae = mean_absolute_error(y_test, y_pred)

print(f"RMSE: {rmse:.4f}")
print(f"MAE: {mae:.4f}")


# COMMAND ----------
mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
print(f"MAPE: {mape:.2f}%")
# COMMAND ----------
