import argparse
import src.data.bronze.forex.extract_data as extract_data
import src.data.silver.forex.lags_features_forex as lags_features_forex
import src.data.silver.forex.mdt_forex as mdt_forex
import time
from src.utils.spark_loader import get_SparkSession

spark = get_SparkSession()


def main(symbol, interval, start_time, end_time, train):
    print("******************************************************************************************************")
    hora_inicio = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print(f"Hora de inicio: {hora_inicio}")

    extract_data.main(spark, symbol, interval, start_time, end_time)
    tiempo_transcurrido = round(time.time() - time.mktime(time.strptime(hora_inicio, '%Y-%m-%d %H:%M:%S')), 2)
    print(f"Tiempo transcurrido: {tiempo_transcurrido} segundos")

    print("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")

    hora_inicio = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print(f"Hora de inicio: {hora_inicio}")
    lags_features_forex.main(spark, symbol, interval)
    tiempo_transcurrido = round(time.time() - time.mktime(time.strptime(hora_inicio, '%Y-%m-%d %H:%M:%S')), 2)
    print(f"Tiempo transcurrido: {tiempo_transcurrido} segundos")

    print("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")

    hora_inicio = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print(f"Hora de inicio: {hora_inicio}")
    mdt_forex.main(symbol, interval, train)
    tiempo_transcurrido = round(time.time() - time.mktime(time.strptime(hora_inicio, '%Y-%m-%d %H:%M:%S')), 2)
    print(f"Tiempo transcurrido: {tiempo_transcurrido} segundos")

    if interval != "1m":
        for i in range(300, 0, -1):
            print(f"Esperando {i} segundos para la siguiente extracción...", end="\r")
            time.sleep(1)
    else:
        for i in range(300, 0, -1):
            print(f"Esperando {i} segundos para la siguiente extracción...", end="\r")
            time.sleep(1)

# main("eurusd", "m5", None, None, False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extracción de datos históricos de Dukascopy.")
    parser.add_argument("--symbol", required=True, help="Símbolo del par de divisas (ej. EURUSD)")
    parser.add_argument("--interval", required=True, help="Intervalo de tiempo (ej. m1, m5)")
    parser.add_argument("--start_time", required=False, default=None, help="Tiempo de inicio (formato: YYYY-MM-DD)")
    parser.add_argument("--end_time", required=False, default=None, help="Tiempo de fin (formato: YYYY-MM-DD)")
    parser.add_argument("--train", required=False, default=False, help="Si se quiere entrenar PCA y Scaler")

    args = parser.parse_args()

    try:
        while True:
            main(args.symbol, args.interval, args.start_time, args.end_time, args.train)
    except KeyboardInterrupt:
        print("Proceso interrumpido por el usuario.")
