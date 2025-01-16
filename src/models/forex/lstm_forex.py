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


def scaler_features(df):
    datetime_column = df["fecha_hora_apertura_dt"]
    target_column = df["monto_cierre_val"]

    features = df.drop(columns=["fecha_hora_apertura_dt", "monto_cierre_val"]).dropna()

    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(features)

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


def df_final():
    df_pd = load_data_forex().toPandas()

    df_scaled = scaler_features(df_pd)

    df_cleaned = remove_redundant_columns(
        df_scaled,
        threshold=0.9,
        exclude_columns=["monto_cierre_val", "fecha_hora_apertura_dt", "monto_maximo_val", "monto_minimo_val", "monto_apertura_val"]
    )

    df_pca = apply_pca(df_cleaned, n_components=0.95)

    df_filtered = selection_features_xgboost(df_pca)
    df_filtered = spark.createDataFrame(df_filtered)
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


df_processed = df_final()
save_postgres(df_processed)
