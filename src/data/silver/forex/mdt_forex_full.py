from src.utils.spark_loader import get_SparkSession
from pyspark.sql import DataFrame
import yaml
from sklearn.decomposition import PCA
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import joblib
import pyspark.sql.functions as F
import psycopg2
from urllib.parse import urlparse

spark = get_SparkSession()

# with open("../../../../conf/local/database_config.yaml", 'r') as stream:
with open("conf/local/database_config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

jdbc_url = config['database']['url']

columns_to_drop = ["intervalo_cd", "par_cd", "monto_apertura_val", "volume"]


def load_data_forex(divisa: str, intervalo: str) -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "forex.features_lags_forex_full")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter(f"intervalo_cd = '{intervalo}'")
        .filter(f"par_cd = '{divisa}'")
        .drop(*columns_to_drop)
        .filter(F.col("fecha_hora_apertura_dt") >= "2022-01-01")
        .drop("intervalo_cd", "par_cd")
        .toPandas()
    )
    return df


def scaler_features(df):
    datetime_column = df["fecha_hora_apertura_dt"]
    target_columns = df[["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]).dropna()

    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(features)

    joblib.dump(scaler, "pkl/scaler_full.pkl")

    scaled_df = pd.DataFrame(scaled_data, columns=features.columns)
    scaled_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)
    scaled_df[["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]] = target_columns.reset_index(drop=True)

    return scaled_df


def apply_pca(df, n_components=0.95):
    target_columns = df[["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]]
    datetime_column = df["fecha_hora_apertura_dt"]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val", "monto_minimo_val", "monto_maximo_val"])

    pca = PCA(n_components=n_components)
    principal_components = pca.fit_transform(features)

    joblib.dump(pca, "pkl/pca_full.pkl")

    print(f"Varianza explicada por las componentes principales: {np.sum(pca.explained_variance_ratio_):.2f}")

    pca_df = pd.DataFrame(principal_components, columns=[f"PC{i+1}" for i in range(principal_components.shape[1])])
    pca_df[["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]] = target_columns.reset_index(drop=True)
    pca_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)

    return pca_df


def transform_with_saved_scaler(df):
    scaler = joblib.load("pkl/scaler_full.pkl")

    datetime_column = df["fecha_hora_apertura_dt"]
    target_columns = df[["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]).dropna()
    scaled_data = scaler.transform(features)

    scaled_df = pd.DataFrame(scaled_data, columns=features.columns)
    scaled_df[["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]] = target_columns.reset_index(drop=True)
    scaled_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)

    return scaled_df


def transform_with_saved_pca(df):
    pca = joblib.load("pkl/pca_full.pkl")

    target_columns = df[["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]]
    datetime_column = df["fecha_hora_apertura_dt"]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val", "monto_minimo_val", "monto_maximo_val"])
    principal_components = pca.transform(features)

    pca_df = pd.DataFrame(principal_components, columns=[f"PC{i+1}" for i in range(principal_components.shape[1])])
    pca_df[["monto_cierre_val", "monto_minimo_val", "monto_maximo_val"]] = target_columns.reset_index(drop=True)
    pca_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)

    return pca_df


def data_trasnform(df: DataFrame, train=True) -> DataFrame:
    df_pd = df

    if train:
        print("Entrenando scaler...")
        df_scaled = scaler_features(df_pd)
    else:
        print("Usando scaler entrenado...")
        df_scaled = transform_with_saved_scaler(df_pd)
    # Aplicar PCA
    if train:
        print("Entrenando PCA...")
        df_pca = apply_pca(df_scaled, n_components=0.95)
        df_pca = spark.createDataFrame(df_pca)
    else:
        print("Usando PCA entrenado...")
        df_pca = transform_with_saved_pca(df_scaled)
        df_pca = spark.createDataFrame(df_pca)

    return df_pca


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
        DELETE FROM forex.mdt_forex_full
        WHERE par_cd = '{divisa}' AND intervalo_cd = '{intervalo}'
        """

    # try:
    #     cursor.execute(delete_query)
    #     conn.commit()
    #     print(
    #         f"Registros eliminados para el rango de fechas y "
    #         f"Par Activos {divisa} con frecuencia {intervalo}"
    #     )
    #     cursor.close()
    #     conn.close()
# 
    # except Exception as e:
    #     print(f"Error al eliminar registros: {e}")
    #     raise


def save_postgres(df: DataFrame, intervalo: str, divisa: str):
    delete_data(intervalo, divisa)

    df = df.withColumn("intervalo_cd", F.lit(intervalo))
    df = df.withColumn("par_cd", F.lit(divisa))

    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "forex.mdt_forex_full") \
        .option("user", config["database"]["user"]) \
        .option("password", config["database"]["password"]) \
        .option("driver", config["database"]["driver"]) \
        .mode("append") \
        .save()


def main(divisa: str, intervalo: str, train: bool):
    df = load_data_forex(divisa, intervalo)
    df_processed = data_trasnform(df, train)
    save_postgres(df_processed, intervalo, divisa)
    print("Proceso de MDT finalizado")


main("eurusd", "m5", True)