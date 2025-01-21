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
import pandas as pd
from tensorflow.keras.callbacks import EarlyStopping

spark = get_SparkSession()

time_steps = 20
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


def prepare_lstm_data(df, target_column="monto_cierre_val", time_steps=time_steps):
    """
    Prepara los datos para el modelo LSTM.
    """
    # Extraer características (sin la columna objetivo ni datetime)
    features = df.drop(columns=["fecha_hora_apertura_dt", target_column])

    X, y = [], []

    print("Inicia preparación de datos")
    for i in range(len(features) - time_steps):
        X.append(features.iloc[i:i + time_steps].values)
        y.append(df[target_column].iloc[i + time_steps])
    print("Preparación terminada")

    return np.array(X), np.array(y)


def build_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(units=75, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.2))
    model.add(LSTM(units=75, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(units=1))

    model.compile(optimizer="adam", loss="mean_squared_error")
    return model


def save_training_columns(df, filepath="training_columns.yaml"):
    columns = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).columns.to_list()
    with open(filepath, "w") as file:
        yaml.dump(columns, file)
    print(f"Columnas de entrenamiento guardadas en {filepath}")


def train_and_predict_lstm(df_final, time_steps=time_steps):
    print("Prepara los datos")
    X, y = prepare_lstm_data(df_final, time_steps=time_steps)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("Construye el modelo")
    model = build_lstm_model(input_shape=(X_train.shape[1], X_train.shape[2]))
    print(model.summary())

    early_stopping = EarlyStopping(
        monitor="val_loss",
        patience=10,
        restore_best_weights=True,
        verbose=1
    )

    print("Entrena el modelo")
    model.fit(X_train, y_train, epochs=50, batch_size=32, validation_split=0.2, callbacks=[early_stopping])

    save_lstm_model(model, model_path=f"lstm_model_{time_steps}.keras")

    # Predicciones
    y_pred = model.predict(X_test)
    return y_test, y_pred


def save_lstm_model(model, model_path=f"lstm_model_{time_steps}.keras"):
    model.save(model_path)
    print(f"Modelo guardado en {model_path}")


def load_lstm_model(model_path="lstm_model"):
    model = tf.keras.models.load_model(model_path)
    print(f"Modelo cargado desde {model_path}")
    return model


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


rmse = np.sqrt(mean_squared_error(y_test, y_pred))
mae = mean_absolute_error(y_test, y_pred)

print(f"RMSE: {rmse:.4f}")
print(f"MAE: {mae:.4f}")

# COMMAND ----------


def save_training_results(df, y_test, y_pred, time_steps=time_steps):
    total_data_points = len(df)
    test_start_index = total_data_points - len(y_test)  # Determinar el índice inicial de y_test
    fechas = df["fecha_hora_apertura_dt"].iloc[test_start_index:].reset_index(drop=True)

    if len(fechas) != len(y_test) or len(y_test) != len(y_pred):
        raise ValueError(
            f"Longitudes no coinciden: fechas({len(fechas)}), y_test({len(y_test)}), y_pred({len(y_pred)})"
        )

    results_df = pd.DataFrame({
        "fecha_hora_apertura_dt": fechas,
        "valor_real": y_test,
        "valor_predicho": y_pred.flatten(),
    })

    spark_df = spark.createDataFrame(results_df)

    spark_df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "public.test_data_modelo_forex") \
        .option("user", config["database"]["user"]) \
        .option("password", config["database"]["password"]) \
        .option("driver", config["database"]["driver"]) \
        .mode("overwrite") \
        .save()

    print("Resultados guardados correctamente en Postgres.")

    return results_df

# COMMAND ----------


df_pd_new = save_training_results(df_processed, y_test, y_pred, time_steps=time_steps)

# COMMAND ----------

mape = np.mean(np.abs((df_pd_new["valor_real"] - df_pd_new["valor_predicho"]) / df_pd_new["valor_real"])) * 100
print(f"MAPE: {mape:.2f}%")
# COMMAND ----------

errores = df_pd_new["valor_real"] - df_pd_new["valor_predicho"]

plt.figure(figsize=(12, 6))
sns.histplot(errores, kde=True, bins=50, label="Distribución de errores")
plt.axvline(0, color="red", linestyle="--", label="Error medio")
plt.legend()
plt.title("Distribución de los errores de predicción")
plt.show()
# COMMAND ----------
