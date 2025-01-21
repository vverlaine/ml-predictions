from src.utils.spark_loader import get_SparkSession
from pyspark.sql import DataFrame
import yaml
# import seaborn as sns
# import matplotlib.pyplot as plt
# from xgboost import XGBRegressor
# from sklearn.model_selection import train_test_split
# from sklearn.feature_selection import SelectFromModel
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

columns_to_drop = ["intervalo_cd", "par_cd", "day_of_week", "hour", "monto_maximo_val", "monto_minimo_val", "monto_apertura_val", "volume"]


def load_data_forex(divisa: str, intervalo: str) -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "forex.features_lags_forex")
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
    target_column = df["monto_cierre_val"]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).dropna()

    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(features)

    joblib.dump(scaler, "pkl/scaler.pkl")

    scaled_df = pd.DataFrame(scaled_data, columns=features.columns)

    scaled_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)
    scaled_df["monto_cierre_val"] = target_column.reset_index(drop=True)

    return scaled_df


def apply_pca(df, n_components=0.95, target_column="monto_cierre_val", datetime_column="fecha_hora_apertura_dt"):
    target_data = df[target_column]
    datetime_data = df[datetime_column]

    features = df.drop(columns=[target_column, datetime_column])

    pca = PCA(n_components=n_components)
    principal_components = pca.fit_transform(features)

    joblib.dump(pca, "pkl/pca.pkl")

    print(f"Varianza explicada por las componentes principales: {np.sum(pca.explained_variance_ratio_):.2f}")

    pca_df = pd.DataFrame(principal_components, columns=[f"PC{i+1}" for i in range(principal_components.shape[1])])

    pca_df[target_column] = target_data.reset_index(drop=True)
    pca_df[datetime_column] = datetime_data.reset_index(drop=True)

    return pca_df


# def remove_redundant_columns(df, threshold, exclude_columns=None):
#     if exclude_columns is None:
#         exclude_columns = []

#     correlation_matrix = df.corr()
#     redundant_columns = set()

#     for i in range(len(correlation_matrix.columns)):
#         for j in range(i):
#             if (
#                 abs(correlation_matrix.iloc[i, j]) >= threshold
#                 and correlation_matrix.columns[j] not in redundant_columns
#                 and correlation_matrix.columns[i] not in exclude_columns
#             ):
#                 redundant_columns.add(correlation_matrix.columns[i])

#     redundant_columns = redundant_columns - set(exclude_columns)

#     print(f"Columnas eliminadas: {list(redundant_columns)}")
#     return df.drop(columns=list(redundant_columns))


# def selection_features_xgboost(df):
#     required_columns = ["monto_cierre_val", "fecha_hora_apertura_dt"]
#     for col in required_columns:
#         if col not in df.columns:
#             raise KeyError(f"La columna requerida '{col}' no se encuentra en el DataFrame.")

#     X = df.drop(columns=required_columns)
#     y = df["monto_cierre_val"]

#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

#     model = XGBRegressor()
#     model.fit(X_train, y_train)

#     joblib.dump(model, "pkl/xgboost.pkl")

#     feature_importances = model.feature_importances_
#     features = X.columns

#     plt.figure(figsize=(10, 8))
#     sns.barplot(y=features, x=feature_importances, order=np.array(features)[np.argsort(feature_importances)])
#     plt.title("Importancia de las características")
#     plt.show()

#     selector = SelectFromModel(model, prefit=True, threshold="median")
#     selected_features = selector.get_support(indices=True)

#     selected_columns = X.columns[selected_features]
#     X_selected = X[selected_columns]

#     X_selected["monto_cierre_val"] = y
#     X_selected["fecha_hora_apertura_dt"] = df["fecha_hora_apertura_dt"]

#     return X_selected


def transform_with_saved_scaler(df):
    scaler = joblib.load("pkl/scaler.pkl")

    datetime_column = df["fecha_hora_apertura_dt"]
    target_column = df["monto_cierre_val"]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).dropna()
    scaled_data = scaler.transform(features)

    scaled_df = pd.DataFrame(scaled_data, columns=features.columns)
    scaled_df["monto_cierre_val"] = target_column.reset_index(drop=True)
    scaled_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)

    return scaled_df


def transform_with_saved_pca(df):
    pca = joblib.load("pkl/pca.pkl")

    datetime_column = df["fecha_hora_apertura_dt"]
    target_column = df["monto_cierre_val"]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).dropna()
    principal_components = pca.transform(features)

    pca_df = pd.DataFrame(principal_components, columns=[f"PC{i+1}" for i in range(principal_components.shape[1])])
    pca_df["monto_cierre_val"] = target_column.reset_index(drop=True)
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

    # # Eliminar columnas redundantes
    # if train:
    #     print("Eliminando columnas redundantes...")
    #     df_cleaned = remove_redundant_columns(
    #         df_pd,
    #         threshold=0.9,
    #         exclude_columns=["monto_cierre_val", "fecha_hora_apertura_dt", "monto_maximo_val", "monto_minimo_val", "monto_apertura_val"]
    #     )
    #     # Guardar las columnas resultantes en un archivo YAML
    #     selected_columns = df_cleaned.columns.to_list()
    #     with open(columns_path, "w") as file:
    #         yaml.dump(selected_columns, file)

    #     df_cleaned = spark.createDataFrame(df_cleaned)
    # else:
    #     print("Cargando columnas seleccionadas...")
    #     with open(columns_path, "r") as file:
    #         selected_columns = yaml.safe_load(file)
    #     # Asegurarse de que solo se usen las columnas guardadas
    #     df_cleaned = df_pd[selected_columns]

    # Aplicar PCA
    if train:
        print("Entrenando PCA...")
        df_pca = apply_pca(df_scaled, n_components=0.95)
        df_pca = spark.createDataFrame(df_pca)
    else:
        print("Usando PCA entrenado...")
        df_pca = transform_with_saved_pca(df_scaled)
        df_pca = spark.createDataFrame(df_pca)

    # # Selección de características con XGBoost
    # if train:
    #     print("Entrenando modelo XGBoost...")
    #     df_filtered = selection_features_xgboost(df_pca)
    #     df_filtered = spark.createDataFrame(df_filtered)
    # else:
    #     print("Usando modelo XGBoost entrenado...")
    #     model = joblib.load(xgboost_path)
    #     required_columns = ["monto_cierre_val", "fecha_hora_apertura_dt"]
    #     X = df_pca.drop(columns=required_columns)
    #     selector = SelectFromModel(model, prefit=True, threshold="median")
    #     selected_features = selector.get_support(indices=True)
    #     selected_columns = X.columns[selected_features]
    #     X_selected = X[selected_columns]
#
    #     X_selected["monto_cierre_val"] = df_pd["monto_cierre_val"].reset_index(drop=True)
    #     X_selected["fecha_hora_apertura_dt"] = df_pd["fecha_hora_apertura_dt"].reset_index(drop=True)
    #     df_filtered = spark.createDataFrame(X_selected)

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
        DELETE FROM forex.mdt_forex
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

    df = df.withColumn("intervalo_cd", F.lit(intervalo))
    df = df.withColumn("par_cd", F.lit(divisa))

    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "forex.mdt_forex") \
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
