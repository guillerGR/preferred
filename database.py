import time
from collections import namedtuple
from sqlite3 import connect, IntegrityError
from typing import NamedTuple

from date_util import timestamp_to_swiss_date, days_to_seconds
from time_factor_calculator import TimeFactorCalculator

BLOOMBERG_DATES_TABLE = "bloomberg_earnings_dates"
NATIVE_DATES_TABLE = "earnings_dates"
READ_FILE_OPEN_MODE = "r"

ListInfoResult = namedtuple("ListInfoResult", ["name", "weight", "parent_list_weight"])
ListComponentDateResult = NamedTuple("ListComponentDateResult", [("name", str), ("ticker", str),
                                                                 ("aggregate_event_value_sign", int),
                                                                 ("latest_change_timestamp", int), ("note", str)])
SecurityInfoResult = namedtuple("SecurityInfoResult", ["ticker", "name", "ir_website"])
PointsResult = NamedTuple("PointsResult", [("name", str), ("ticker", str), ("ir_website", str),
                                           ("aggregate_points_value", int)])
TimeWeightedPointsResult = NamedTuple("TimeWeightedPointsResult", [("name", str), ("ticker", str), ("event_value", int),
                                                                   ("event_date_timestamp", int)])
ListHistoryResult = NamedTuple("ListHistoryResult", [("name", str), ("ticker", str), ("event_name", str),
                                                     ("event_date_timestamp", int)])
SecurityResult = namedtuple("SecurityResult", ["name", "country", "currency", "country_weight", "currency_weight",
                                               "ir_website"])
SecurityHistoryResult = NamedTuple("SecurityHistoryResult", [("list_name", str), ("event_name", str),
                                                             ("event_date_timestamp", int), ("pref_list", str),
                                                             ("event_note", str)])
SecurityCountryResult = namedtuple("SecurityCountryResult", ["ticker", "name", "ir_website", "country"])


class Database:
    def __init__(self, path):
        self.path = path
        self.connection = None
        self.cursor = None

        try:
            # sqlite will create a new db if file doesn't exist, check before connecting
            open(self.path, READ_FILE_OPEN_MODE).close()
        except IOError:
            raise IOError("Couldn't find database file.")

    def connect(self):
        self.connection = connect(self.path)
        self.connection.text_factory = str
        self.cursor = self.connection.cursor()
        print(f"Database connection established: {self.path}")

    def close(self):
        self.cursor.close()
        self.connection.close()
        print(f"Database connection closed: {self.path}")

    def run_query(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_primary_key_for_table(self, table_name):
        columns_in_table = self.run_query(f"PRAGMA table_info({table_name})")
        cid, name, column_type, notnull, dflt_value, pk = next(column for column in columns_in_table)
        if pk == 1:
            return name

    def get_primary_key_value_for_ticker(self, table_name, ticker):
        return self.get_primary_key_value_for_column(table_name, "ticker", ticker)

    def get_primary_key_value_for_column(self, table_name, column_name, column_value):
        primary_key_column_name = self.get_primary_key_for_table(table_name)
        row_matches_list = self.run_query(f"SELECT {primary_key_column_name} FROM {table_name} "
                                          f"WHERE {column_name} = '{column_value}'")

        if len(row_matches_list) == 1:
            primary_key_column_value = row_matches_list[0][0]
            return primary_key_column_value
        else:
            raise ValueError(f"Found no primary key match for value: {column_value}")

    def insert_security(self, security):
        try:
            country_id = self.get_primary_key_value_for_ticker("countries", security.country)
            currency_id = self.get_primary_key_value_for_ticker("currencies", security.currency)
        except Exception as e:
            print(f"Could not insert security. Reason: {e}")

        try:
            self.cursor.execute("INSERT INTO securities(name, ticker, country_id, currency_id, ir_website) "
                                "VALUES (?, ?, ?, ?, ?)",
                                (security.name, security.ticker, country_id, currency_id, security.ir_website))
            self.connection.commit()
        except IntegrityError as e:
            print(f"Integrity Error: {e}, primary key value: {security.ticker}")

    def insert_earnings_date(self, earnings_date):
        try:
            security_id = self.get_primary_key_value_for_ticker("securities", earnings_date.security)
        except Exception as e:
            print(f"Could not insert earnings date. Reason: {e}")

        self.cursor.execute("INSERT INTO earnings_dates(security_id, date_epoch) VALUES (?, ?)",
                            (security_id, earnings_date.timestamp))
        self.connection.commit()

    def insert_list_change(self, list_change):
        try:
            security_id = self.get_primary_key_value_for_ticker("securities", list_change.security_ticker)
            list_id = self.get_primary_key_value_for_ticker("lists", list_change.pref_list)
            event_id = self.get_primary_key_value_for_ticker("list_change_events", list_change.event)
        except Exception as e:
            print(f"Could not insert list change. Reason: {e}")
            return

        self.cursor.execute("INSERT INTO list_changes(security_id, list_id, event_id, date_epoch) "
                            "VALUES (?, ?, ?, ?)", (security_id, list_id, event_id, list_change.timestamp))
        self.connection.commit()

    def query_list_info(self, pref_list):
        list_info = self.run_query("SELECT l1.name, w1.name, w2.name FROM lists l1, weights w1, weights w2 "
                                   "LEFT JOIN lists l2 ON l1.parent_list_id = l2.list_id "
                                   f"WHERE l1.ticker = '{pref_list}' AND l1.weight_id = w1.weight_id "
                                   "AND l2.weight_id = w2.weight_id")

        if len(list_info) != 1:
            raise Exception(f"Did not find unique list for list {pref_list}")
        return next(map(ListInfoResult._make, list_info))

    def query_list_components(self, pref_list):
        list_components = self.run_query(
            "SELECT s.name, s.ticker, SUM(e.value_sign), MAX(c.date_epoch), c.note FROM lists l, securities s, "
            f"list_changes c, list_change_events e WHERE l.ticker = '{pref_list}' AND e.event_id = c.event_id "
            "AND c.list_id = l.list_id AND s.security_id = c.security_id GROUP BY s.ticker ORDER BY s.name ASC")

        parsed_components = map(ListComponentDateResult._make, list_components)
        return [component for component in parsed_components if component.aggregate_event_value_sign > 0]

    def print_dates_summary(self, ticker):
        next_date = self.safe_timestamp_to_swiss_date(self.query_next_earnings_date_for_ticker(ticker))
        prev_date = self.safe_timestamp_to_swiss_date(self.query_previous_earnings_date_for_ticker(ticker))
        bloomberg_date = self.safe_timestamp_to_swiss_date(self.query_newest_earnings_date_for_ticker(ticker))

        summary = f"[B: {bloomberg_date}, N: {next_date}, P: {prev_date}"
        if (bloomberg_date is None and next_date) is None or prev_date is None:
            return f"{summary}, URL: {self.query_url_for_ticker(ticker)}]"
        else:
            return f"{summary}]"

    @staticmethod
    def safe_timestamp_to_swiss_date(next_timestamp):
        return None if next_timestamp is None else timestamp_to_swiss_date(next_timestamp)

    def query_next_earnings_date_for_ticker(self, ticker):
        current_time = int(time.time())
        results = self.run_query("SELECT MIN(d.date_epoch) FROM securities s, earnings_dates d "
                                 f"WHERE s.ticker = '{ticker}' AND d.security_id = s.security_id "
                                 f"AND d.date_epoch > {current_time}")

        return results[0][0] if results[0][0] else None

    def query_previous_earnings_date_for_ticker(self, ticker):
        current_time = int(time.time())
        results = self.run_query("SELECT MAX(d.date_epoch) FROM securities s, earnings_dates d "
                                 f"WHERE s.ticker = '{ticker}' AND d.security_id = s.security_id "
                                 f"AND d.date_epoch < {current_time}")

        return results[0][0] if results[0][0] else None

    def query_newest_earnings_date_for_ticker(self, ticker):
        results = self.run_query("SELECT b.date_epoch, MAX(b.bloomberg_date_id) FROM securities s, "
                                 f"bloomberg_earnings_dates b WHERE s.ticker = '{ticker}' "
                                 "AND b.security_id = s.security_id GROUP BY s.ticker")

        return results[0][0] if results else None

    def query_url_for_ticker(self, ticker):
        results = self.run_query(f"SELECT ir_website FROM securities WHERE ticker = '{ticker}'")

        return results[0][0]

    def query_security_info(self, name):
        return map(SecurityInfoResult._make,
                   self.run_query(f"SELECT ticker, name, ir_website FROM securities WHERE name LIKE '%%{name}%%'"))

    def query_list_of_lists(self, pref_list):
        children_lists = self.run_query(f"SELECT l1.ticker FROM lists l1, lists l2 WHERE l2.ticker = '{pref_list}' "
                                        "AND l1.parent_list_id = l2.list_id")

        return [child_list[0] for child_list in children_lists]

    def query_points(self, threshold_days, ticker=None):
        current_time = int(time.time())
        threshold_time = current_time - days_to_seconds(threshold_days)
        ticker_filter = "" if ticker is None else f"AND s.ticker = '{ticker}'"

        points = self.run_query("SELECT s.name, s.ticker, s.ir_website, SUM(e.value) as points FROM securities s, "
                                "list_changes c, list_change_events e WHERE e.event_id = c.event_id "
                                f"AND s.security_id = c.security_id AND c.date_epoch > {threshold_time} "
                                f"{ticker_filter} GROUP BY s.ticker ORDER BY points DESC")
        return map(PointsResult._make, points)

    def query_time_weighted_points(self, threshold_days, ticker=None, country=None):
        current_time = int(time.time())
        threshold_seconds = days_to_seconds(threshold_days)
        threshold_time = current_time - threshold_seconds
        country_id = None if country is None else self.get_primary_key_value_for_ticker("countries", country)
        ticker_filter = "" if ticker is None else f"AND s.ticker = '{ticker}'"
        country_filter = "" if country_id is None else f"AND c.country_id = '{country_id}'"

        results = self.run_query("SELECT s.name, s.ticker, e.value, lc.date_epoch FROM securities s, "
                                 "list_changes lc, list_change_events e, countries c WHERE e.event_id = lc.event_id "
                                 f"AND s.security_id = lc.security_id AND lc.date_epoch > {threshold_time} "
                                 f"AND s.country_id = c.country_id {ticker_filter} {country_filter} ORDER BY s.ticker")

        factor_calculator = TimeFactorCalculator(current_time, threshold_seconds)
        return map(TimeWeightedPointsResult._make, results), factor_calculator

    def query_list_history(self, pref_list):
        histories = self.run_query(
            "SELECT s.name, s.ticker, e.name, c.date_epoch FROM lists l, securities s, list_changes c, "
            f"list_change_events e WHERE l.ticker = '{pref_list}' AND e.event_id = c.event_id "
            "AND c.list_id = l.list_id AND s.security_id = c.security_id ORDER BY c.date_epoch DESC")

        return map(ListHistoryResult._make, histories)

    def query_earnings_dates(self, ticker, table):
        dates = self.run_query(f"SELECT d.date_epoch FROM securities s, {table} d WHERE s.security_id = d.security_id "
                               f"AND s.ticker = '{ticker}' ORDER BY d.date_epoch DESC")

        return [int(date[0]) for date in dates]

    def query_native_earnings_dates(self, ticker):
        return self.query_earnings_dates(ticker, NATIVE_DATES_TABLE)

    def query_bloomberg_earnings_dates(self, ticker):
        return self.query_earnings_dates(ticker, BLOOMBERG_DATES_TABLE)

    def query_security(self, ticker):
        securities = self.run_query(
            "SELECT s.name, co.name, cu.ticker, cow.name, cuw.name, s.ir_website FROM securities s, "
            f"countries co, currencies cu, weights cow, weights cuw WHERE s.ticker = '{ticker}' "
            "AND s.country_id = co.country_id AND s.currency_id = cu.currency_id "
            "AND co.weight_id = cow.weight_id AND cu.weight_id = cuw.weight_id")

        return next(map(SecurityResult._make, securities))

    def query_history(self, ticker):
        histories = self.run_query("SELECT l.name, e.name, c.date_epoch, l.ticker, c.note FROM lists l, securities s, "
                                   f"list_changes c, list_change_events e WHERE s.ticker = '{ticker}' "
                                   "AND e.event_id = c.event_id AND c.list_id = l.list_id "
                                   "AND s.security_id = c.security_id ORDER BY l.ticker ASC, c.date_epoch DESC")

        return map(SecurityHistoryResult._make, histories)

    def query_all_securities(self):
        securities = self.run_query("SELECT s.ticker, s.name, s.ir_website, c.name FROM securities s, countries c WHERE "
                                 "s.country_id = c.country_id")
        securities += self.run_query("SELECT s.ticker, a.alt_name, s.ir_website, c.name FROM securities s, "
                                  "securities_alt_names a, countries c WHERE a.security_id = s.security_id AND "
                                  "s.country_id = c.country_id")

        return map(SecurityCountryResult._make, securities)
