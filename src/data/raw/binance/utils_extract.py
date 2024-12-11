import requests


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


def get_all_historical_klines(symbol, interval, start_time, end_time):
    df_list = []
    while start_time < end_time:
        new_data = get_historical_klines(symbol, interval, start_time, end_time)
        if not new_data:
            break
        df_list.extend(new_data)
        start_time = new_data[-1][0] + 1
    return df_list
