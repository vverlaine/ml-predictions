# COMMAND ----------
import yaml
from pyspark.sql import DataFrame
from src.utils.spark_loader import get_SparkSession
import lightgbm as lgb
from sklearn.model_selection import train_test_split
# from sklearn.metrics import mean_squared_error
from lightgbm import early_stopping
from sklearn.metrics import mean_absolute_error, r2_score
# import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------
spark = get_SparkSession()

with open("conf/local/database_config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

jdbc_url = config['database']['url']

COLUMNS = [
    "monto_apertura_val",
    "monto_maximo_val",
    "monto_minimo_val",
    "volumen_criptomoneda_negociado_val",
    "volumen_moneda_negociado_val",
    "volumen_criptomoneda_comprado_val",
    "volumen_moneda_comprado_val"
]


def extract_data() -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.mdt_binance")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter("fecha_hora_apertura_dt >= '2024-06-01'")
        .drop(*COLUMNS)
        .toPandas()
    )
    return df


df = extract_data()

# COMMAND ----------
# Eliminar la columna de fecha si no es necesaria
df = df.drop(columns=["fecha_hora_apertura_dt"])

# Separar características y objetivo
X = df.drop(columns=["monto_cierre_val"])
y = df["monto_cierre_val"]

# COMMAND ----------
# Dividir los datos en entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=142)

# COMMAND ----------

train_data = lgb.Dataset(X_train, label=y_train)
test_data = lgb.Dataset(X_test, label=y_test, reference=train_data)

# COMMAND ----------

# Configuración del modelo
params = {
    "objective": "regression",
    "metric": "rmse",
    "learning_rate": 0.01,
    "max_depth": 6,
    "num_leaves": 32,
    "boosting_type": "gbdt",
    "verbose": -1,
    "numIterations": 1000
}

# Entrenar el modelo
model = lgb.train(
    params,
    train_data,
    valid_sets=[train_data, test_data],
    num_boost_round=1000,
    callbacks=[early_stopping(stopping_rounds=100)]
)
# COMMAND ----------
# Predicciones
y_pred = model.predict(X_test, num_iteration=model.best_iteration)

# COMMAND ----------
# Calcular métricas adicionales
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Mean Absolute Error (MAE): {mae}")
print(f"R2 Score: {r2}")

# COMMAND ----------
# Obtener nombres de las características y sus importancias
feature_importance_df = pd.DataFrame({
    'feature': model.feature_name(),
    'importance': model.feature_importance()
})

# Ordenar por importancia descendente
feature_importance_df = feature_importance_df.sort_values(by='importance', ascending=False)

# COMMAND ----------
# Exportar a un archivo CSV
feature_importance_df.to_csv('feature_importance.csv', index=False)
# COMMAND ----------
