from datetime import datetime


def estimate_data_points(start_time: datetime, end_time: datetime, interval_sec: float):
    if interval_sec <= 0:
        raise ValueError('interval_sec must be greater than 0')
    return int((end_time - start_time).total_seconds() / interval_sec)
