from src.utils.spark_loader import get_SparkSession
from pyspark.sql import DataFrame
import yaml
import seaborn as sns
import matplotlib.pyplot as plt
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import SelectFromModel
from sklearn.decomposition import PCA
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import joblib

spark = get_SparkSession()

# with open("../../../../conf/local/database_config.yaml", 'r') as stream:
with open("conf/local/database_config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)

jdbc_url = config['database']['url']


def load_data_forex() -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.mdt_forex_data_m5")
        .option("user", config["database"]["user"])
        .option("password", config["database"]["password"])
        .option("driver", config["database"]["driver"])
        .load()
        .filter("intervalo_cd = 'm5'")
        .drop("intervalo_cd", "par_cd", "day_of_week", "hour")
    )
    return df


def scaler_features(df, scaler_path="scaler.pkl"):
    datetime_column = df["fecha_hora_apertura_dt"]
    target_column = df["monto_cierre_val"]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).dropna()

    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(features)

    joblib.dump(scaler, scaler_path)

    scaled_df = pd.DataFrame(scaled_data, columns=features.columns)

    scaled_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)
    scaled_df["monto_cierre_val"] = target_column.reset_index(drop=True)

    return scaled_df


def apply_pca(df, n_components=0.95, target_column="monto_cierre_val", datetime_column="fecha_hora_apertura_dt", pca_path="pca.pkl"):
    target_data = df[target_column]
    datetime_data = df[datetime_column]

    features = df.drop(columns=[target_column, datetime_column])

    pca = PCA(n_components=n_components)
    principal_components = pca.fit_transform(features)

    joblib.dump(pca, pca_path)

    print(f"Varianza explicada por las componentes principales: {np.sum(pca.explained_variance_ratio_):.2f}")

    pca_df = pd.DataFrame(principal_components, columns=[f"PC{i+1}" for i in range(principal_components.shape[1])])

    pca_df[target_column] = target_data.reset_index(drop=True)
    pca_df[datetime_column] = datetime_data.reset_index(drop=True)

    return pca_df


def remove_redundant_columns(df, threshold, exclude_columns=None):
    if exclude_columns is None:
        exclude_columns = []

    correlation_matrix = df.corr()
    redundant_columns = set()

    for i in range(len(correlation_matrix.columns)):
        for j in range(i):
            if (
                abs(correlation_matrix.iloc[i, j]) >= threshold
                and correlation_matrix.columns[j] not in redundant_columns
                and correlation_matrix.columns[i] not in exclude_columns
            ):
                redundant_columns.add(correlation_matrix.columns[i])

    redundant_columns = redundant_columns - set(exclude_columns)

    print(f"Columnas eliminadas: {list(redundant_columns)}")
    return df.drop(columns=list(redundant_columns))


def selection_features_xgboost(df):
    required_columns = ["monto_cierre_val", "fecha_hora_apertura_dt"]
    for col in required_columns:
        if col not in df.columns:
            raise KeyError(f"La columna requerida '{col}' no se encuentra en el DataFrame.")

    X = df.drop(columns=required_columns)
    y = df["monto_cierre_val"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = XGBRegressor()
    model.fit(X_train, y_train)

    joblib.dump(model, "xgboost.pkl")

    feature_importances = model.feature_importances_
    features = X.columns

    plt.figure(figsize=(10, 8))
    sns.barplot(y=features, x=feature_importances, order=np.array(features)[np.argsort(feature_importances)])
    plt.title("Importancia de las características")
    plt.show()

    selector = SelectFromModel(model, prefit=True, threshold="median")
    selected_features = selector.get_support(indices=True)

    selected_columns = X.columns[selected_features]
    X_selected = X[selected_columns]

    X_selected["monto_cierre_val"] = y
    X_selected["fecha_hora_apertura_dt"] = df["fecha_hora_apertura_dt"]

    return X_selected


def transform_with_saved_scaler(df, scaler_path="scaler.pkl"):
    """
    Transforma un nuevo conjunto de datos utilizando un scaler previamente ajustado.
    """
    # Cargar el scaler guardado
    scaler = joblib.load(scaler_path)

    # Guardar las columnas de fecha y target
    datetime_column = df["fecha_hora_apertura_dt"]
    target_column = df["monto_cierre_val"]

    # Escalar características
    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).dropna()
    scaled_data = scaler.transform(features)

    # Crear DataFrame con datos escalados
    scaled_df = pd.DataFrame(scaled_data, columns=features.columns)
    scaled_df["monto_cierre_val"] = target_column.reset_index(drop=True)
    scaled_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)

    return scaled_df


def transform_with_saved_pca(df, pca_path="pca.pkl"):
    """
    Aplica PCA a un conjunto de datos utilizando un modelo PCA previamente ajustado.
    """
    # Cargar el modelo PCA guardado
    pca = joblib.load(pca_path)

    # Guardar las columnas de fecha y target
    datetime_column = df["fecha_hora_apertura_dt"]
    target_column = df["monto_cierre_val"]

    # Aplicar PCA
    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).dropna()
    principal_components = pca.transform(features)

    # Crear DataFrame con componentes principales
    pca_df = pd.DataFrame(principal_components, columns=[f"PC{i+1}" for i in range(principal_components.shape[1])])
    pca_df["monto_cierre_val"] = target_column.reset_index(drop=True)
    pca_df["fecha_hora_apertura_dt"] = datetime_column.reset_index(drop=True)

    return pca_df


def df_final(train=True, scaler_path="scaler.pkl", pca_path="pca.pkl", xgboost_path="xgboost.pkl", columns_path="columns.yaml"):
    # Cargar datos desde Spark
    df_pd = load_data_forex().toPandas()

    # Escalar los datos
    if train:
        print("Entrenando scaler...")
        df_scaled = scaler_features(df_pd, scaler_path=scaler_path)
    else:
        print("Usando scaler entrenado...")
        df_scaled = transform_with_saved_scaler(df_pd, scaler_path=scaler_path)

    # Eliminar columnas redundantes
    if train:
        print("Eliminando columnas redundantes...")
        df_cleaned = remove_redundant_columns(
            df_scaled,
            threshold=0.9,
            exclude_columns=["monto_cierre_val", "fecha_hora_apertura_dt", "monto_maximo_val", "monto_minimo_val", "monto_apertura_val"]
        )
        # Guardar las columnas resultantes en un archivo YAML
        selected_columns = df_cleaned.columns.to_list()
        with open(columns_path, "w") as file:
            yaml.dump(selected_columns, file)
    else:
        print("Cargando columnas seleccionadas...")
        with open(columns_path, "r") as file:
            selected_columns = yaml.safe_load(file)
        # Asegurarse de que solo se usen las columnas guardadas
        df_cleaned = df_scaled[selected_columns]

    # Aplicar PCA
    if train:
        print("Entrenando PCA...")
        df_pca = apply_pca(df_cleaned, n_components=0.95, pca_path=pca_path)
    else:
        print("Usando PCA entrenado...")
        df_pca = transform_with_saved_pca(df_cleaned, pca_path=pca_path)

    # Selección de características con XGBoost
    if train:
        print("Entrenando modelo XGBoost...")
        df_filtered = selection_features_xgboost(df_pca)
        df_filtered = spark.createDataFrame(df_filtered)
    else:
        print("Usando modelo XGBoost entrenado...")
        model = joblib.load(xgboost_path)
        required_columns = ["monto_cierre_val", "fecha_hora_apertura_dt"]
        X = df_pca.drop(columns=required_columns)
        selector = SelectFromModel(model, prefit=True, threshold="median")
        selected_features = selector.get_support(indices=True)
        selected_columns = X.columns[selected_features]
        X_selected = X[selected_columns]

        X_selected["monto_cierre_val"] = df_pca["monto_cierre_val"].reset_index(drop=True)
        X_selected["fecha_hora_apertura_dt"] = df_pca["fecha_hora_apertura_dt"].reset_index(drop=True)
        df_filtered = spark.createDataFrame(X_selected)

    return df_filtered


def save_postgres(df):
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "public.data_modelo_forex") \
        .option("user", config["database"]["user"]) \
        .option("password", config["database"]["password"]) \
        .option("driver", config["database"]["driver"]) \
        .mode("overwrite") \
        .save()


# Entrenamiento inicial
# df_processed = df_final(
#     train=True,
#     scaler_path="scaler.pkl",
#     pca_path="pca.pkl",
#     xgboost_path="xgboost.pkl"
# )
# # Guarda los datos procesados en Postgres (u otro destino)
# save_postgres(df_processed)
# print("Entrenamiento inicial completado. Modelos guardados.")

# Transformación de nuevos datos
df_transformed = df_final(
    train=False,
    scaler_path="scaler.pkl",
    pca_path="pca.pkl",
    xgboost_path="xgboost.pkl"
)

# Guarda los datos transformados en Postgres (u otro destino)
save_postgres(df_transformed)

print("Transformación completada. Datos procesados listos.")
