from datetime import datetime, timedelta


def get_unix_time(START_TIME: str, END_TIME: str):
    if START_TIME is None or END_TIME is None:
        END_TIME = datetime.now()
        START_TIME = END_TIME - timedelta(days=7)
        END_TIME = END_TIME

    else:
        START_TIME = datetime.strptime(START_TIME, '%Y-%m-%d %H:%M')
        END_TIME = datetime.strptime(END_TIME, '%Y-%m-%d %H:%M')

    INT_START_TIME = int(START_TIME.timestamp() * 1000)
    INT_END_TIME = int(END_TIME.timestamp() * 1000)

    START_TIME = START_TIME.strftime('%Y-%m-%d %H:%M')
    END_TIME = END_TIME.strftime('%Y-%m-%d %H:%M')

    return START_TIME, END_TIME, INT_START_TIME, INT_END_TIME
