from datetime import timedelta, datetime
from typing import List, Dict


def to_start_of_day(date_time: datetime):
    """
    :param date_time: datetime
    :return: to start of day
    """
    start_of_day = date_time.replace(microsecond=0, second=0, minute=0)
    return start_of_day

def to_start_of_hour(date_time: datetime):
    start_of_hour = date_time.replace(microsecond=0, second=0)
    return start_of_hour

now = datetime.now()

print(now)

print(to_start_of_hour(now))