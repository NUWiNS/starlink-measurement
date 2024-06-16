import pytz
from datetime import datetime


def format_datetime_as_iso_8601(dt: datetime):
    """
    :param dt:
    :return:
    """
    return dt.isoformat()


def append_timezone(dt: datetime, timezone_str: str, is_dst: bool = True):
    timezone = pytz.timezone(timezone_str)
    dt_aware = timezone.localize(dt, is_dst=is_dst)  # is_dst=True for daylight saving time
    return dt_aware
