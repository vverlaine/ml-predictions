import requests
import pandas as pd
import pytz


def get_historical_klines(symbol, interval, start_time, end_time):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data


def parse_klines_data(data):
    guatemala_tz = pytz.timezone("America/Guatemala")

    df = pd.DataFrame(
        data,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ],
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize('UTC').dt.tz_convert(guatemala_tz)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms").dt.tz_localize('UTC').dt.tz_convert(guatemala_tz)
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["quote_asset_volume"] = df["quote_asset_volume"].astype(float)
    df["taker_buy_base_asset_volume"] = df["taker_buy_base_asset_volume"].astype(float)
    df["taker_buy_quote_asset_volume"] = df["taker_buy_quote_asset_volume"].astype(float)

    return df


def get_all_historical_klines(symbol, interval, start_time, end_time):
    df_list = []
    while start_time < end_time:
        new_data = get_historical_klines(symbol, interval, start_time, end_time)
        if not new_data:
            break
        df_list.extend(new_data)
        start_time = new_data[-1][0] + 1

    df_pd = parse_klines_data(df_list)

    return df_pd
