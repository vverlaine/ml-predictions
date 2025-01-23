# COMMAND ----------
# Importaciones necesarias
from src.utils.spark_loader import get_SparkSession
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
import numpy as np
import matplotlib.pyplot as plt
import yaml
from pyspark.sql import DataFrame
from sklearn.metrics import mean_squared_error, mean_absolute_error
import pandas as pd
from tensorflow.keras.callbacks import EarlyStopping

spark = get_SparkSession()
# COMMAND ----------
time_steps = 20
# Leer configuración de base de datos
with open("conf/local/database_config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

jdbc_url = config['database']['url']

intervalo = "m5"
par_cd = "eurusd"


# Cargar datos desde la base de datos
def load_data_forex() -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "forex.mdt_forex_full")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter(f"intervalo_cd = '{intervalo}'")
        .filter(f"par_cd = '{par_cd}'")
        .drop("intervalo_cd", "par_cd")
        .orderBy("fecha_hora_apertura_dt")
    )
    return df


# Preparar los datos para múltiples variables objetivo
def prepare_lstm_data(df, target_columns=["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"], time_steps=time_steps):
    """
    Prepara los datos para el modelo LSTM con múltiples variables objetivo.
    """
    features = df.drop(columns=["fecha_hora_apertura_dt"] + target_columns)
    targets = df[target_columns]

    X, y = [], []

    print("Inicia preparación de datos")
    for i in range(len(features) - time_steps):
        X.append(features.iloc[i:i + time_steps].values)
        y.append(targets.iloc[i + time_steps].values)  # Cada fila tendrá todas las variables objetivo
    print("Preparación terminada")

    return np.array(X), np.array(y)


# Construir el modelo LSTM para múltiples salidas
def build_lstm_model(input_shape, output_units):
    """
    Construye un modelo LSTM que predice múltiples variables objetivo.
    """
    model = Sequential()
    model.add(LSTM(units=75, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.2))
    model.add(LSTM(units=75, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(units=output_units))  # Salida con unidades igual al número de variables objetivo

    model.compile(optimizer="adam", loss="mean_squared_error")
    return model


# Entrenar y predecir
def train_and_predict_lstm(df_final, time_steps=time_steps):
    print("Prepara los datos")
    target_columns = ["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]
    X, y = prepare_lstm_data(df_final, target_columns=target_columns, time_steps=time_steps)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("Construye el modelo")
    model = build_lstm_model(input_shape=(X_train.shape[1], X_train.shape[2]), output_units=len(target_columns))
    print(model.summary())

    early_stopping = EarlyStopping(
        monitor="val_loss",
        patience=10,
        restore_best_weights=True,
        verbose=1
    )

    print("Entrena el modelo")
    model.fit(X_train, y_train, epochs=50, batch_size=32, validation_split=0.2, callbacks=[early_stopping])

    save_lstm_model(model, model_path=f"lstm_model_{time_steps}_{intervalo}_{par_cd}.keras")

    # Predicciones
    y_pred = model.predict(X_test)
    return y_test, y_pred, target_columns


# Guardar el modelo entrenado
def save_lstm_model(model, model_path=f"lstm_model_{time_steps}_full.keras"):
    model.save(model_path)
    print(f"Modelo guardado en {model_path}")


# Guardar los resultados de entrenamiento
def save_training_results(df, y_test, y_pred, target_columns, time_steps=time_steps):
    total_data_points = len(df)
    test_start_index = total_data_points - len(y_test)
    fechas = df["fecha_hora_apertura_dt"].iloc[test_start_index:].reset_index(drop=True)

    if len(fechas) != len(y_test) or len(y_test) != len(y_pred):
        raise ValueError(
            f"Longitudes no coinciden: fechas({len(fechas)}), y_test({len(y_test)}), y_pred({len(y_pred)})"
        )

    results = {
        "fecha_hora_apertura_dt": fechas
    }

    for i, column in enumerate(target_columns):
        results[f"real_{column}"] = y_test[:, i]
        results[f"prediccion_{column}"] = y_pred[:, i]

    results_df = pd.DataFrame(results)
    spark_df = spark.createDataFrame(results_df)

    spark_df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "public.test_data_modelo_forex_full") \
        .option("user", config["database"]["user"]) \
        .option("password", config["database"]["password"]) \
        .option("driver", config["database"]["driver"]) \
        .mode("overwrite") \
        .save()

    print("Resultados guardados correctamente en Postgres.")

    return results_df


# COMMAND ----------
# Entrenamiento y evaluación
df_processed = load_data_forex().toPandas()
y_test, y_pred, target_columns = train_and_predict_lstm(df_processed)
# COMMAND ----------
# Guardar resultados
df_results = save_training_results(df_processed, y_test, y_pred, target_columns)
# COMMAND ----------
# Métricas de evaluación
for i, column in enumerate(target_columns):
    rmse = np.sqrt(mean_squared_error(y_test[:, i], y_pred[:, i]))
    mae = mean_absolute_error(y_test[:, i], y_pred[:, i])
    print(f"{column} - RMSE: {rmse:.4f}, MAE: {mae:.4f}")

# Visualización de predicciones
for i, column in enumerate(target_columns):
    plt.figure(figsize=(12, 6))
    plt.plot(y_test[:50, i], label=f"Real {column}")
    plt.plot(y_pred[:50, i], label=f"Predicción {column}")
    plt.legend()
    plt.title(f"Predicción de {column}")
    plt.show()

# COMMAND ----------
