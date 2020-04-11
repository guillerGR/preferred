import datetime
import time

EPOCH = datetime.datetime(1970, 1, 1)
SECONDS_IN_A_DAY = 24 * 60 * 60


def swiss_date_to_timestamp(date):
    # example: April 11th 2086 = 11.04.86
    parsed_date = datetime.datetime.strptime(date, "%d.%m.%y")
    return (parsed_date - EPOCH).total_seconds()


def timestamp_to_swiss_date(epoch):
    parsed_epoch = datetime.datetime.fromtimestamp(epoch)
    return parsed_epoch.strftime("%d.%m.%y")


def days_to_seconds(days):
    return days * SECONDS_IN_A_DAY


def time_range_from_now(days):
    current_time = int(time.time())
    threshold_time = current_time + days_to_seconds(days)

    lower_timestamp = min(current_time, threshold_time)
    higher_timestamp = max(current_time, threshold_time)
    return lower_timestamp, higher_timestamp
