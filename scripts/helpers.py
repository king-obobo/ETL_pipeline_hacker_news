from __future__ import annotations
from datetime import datetime, timedelta, timezone


"""Helper functions for the ETL pipeline."""



# ==================== DATE HELPER FUNCS ============================== 
def parse_date(date_str: str) -> datetime:
    """
    Must be in the format 'YYYY-MM-DD'-> midnight UTC (always timezone-aware)

    Args:
        date_str (str): date to be parsed as a string

    Returns:
        datetime: string converted to datetime
    """
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo= timezone.utc)


def to_epoch(dt: datetime) -> int:
    """
    Converts a datetime object to epoch seconds.

    Args:
        dt (datetime): datetime object to be converted

    Returns:
        int: epoch seconds
    """
    return int(dt.timestamp())


def date_to_epoch(date_str: str) -> int:
    """
    Converts a date string in 'YYYY-MM-DD' format to epoch seconds.

    Args:
        date_str (str): date to be converted as a string

    Returns:
        int: epoch seconds
    """
    return to_epoch(parse_date(date_str))


def build_windows(start: datetime, days: int, window_hours: int) -> list[tuple[datetime, datetime]]:
    """
    Builds a list of time windows in epoch seconds.

    Args:
        start (datetime): starting datetime for the first window
        days (int): number of days to create windows for
        window_hours (int): size of each window in hours

    Returns:
        list[tuple[datetime, datetime]]: list of tuples containing start and end datetime objects for each window
    """
    if window_hours <= 0 or 24 % window_hours != 0:
        raise ValueError("window_hours must be a positive divisor of 24. For example, 1, 2, 3, 4, 6, 8, 12, or 24.")
    step = timedelta(hours=window_hours)
    return [
        (start + i * step, start + (i + 1) * step)
        for i in range(days * 24 // window_hours)
    ]
