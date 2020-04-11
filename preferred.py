from database import Database
from date_util import timestamp_to_swiss_date
from earnings_date import EarningsDate
from list_change import ListChange
from menu import MenuOption, Menu
from security import Security
from weighted_points_processor import WeightedPointsProcessor, AggregatedValue

MENU_OPTIONS = [MenuOption(["s"], "add_security"), MenuOption(["e", "ed"], "add_earnings_date"),
                MenuOption(["u"], "query_upcoming_earnings"), MenuOption(["l", "lc"], "add_list_change"),
                MenuOption(["hs"], "query_history"), MenuOption(["ls"], "query_list_components"),
                MenuOption(["miss"], "query_missing_earnings"), MenuOption(["t"], "query_security"),
                MenuOption(["sl"], "query_lists_for_security"), MenuOption(["ll"], "query_list_of_lists"),
                MenuOption(["move"], "add_move"), MenuOption(["note"], "add_note"), MenuOption(["p"], "query_points"),
                MenuOption(["twp"], "query_time_weighted_points"), MenuOption(["b"], "add_bloomberg_scrape_date"),
                MenuOption(["hl"], "query_list_history"), MenuOption(["d"], "switch_databases"),
                MenuOption(["he"], "query_earnings_for_ticker"), MenuOption(["a"], "query_ticker"),
                MenuOption(["ra"], "add_analyst"), MenuOption(["salt"], "add_security_alt_name"),
                MenuOption(["q"], "clean_up")]
PRIVATE_DB_PATH = "private.db"
PUBLIC_DB_PATH = "public.db"
POINTS_DAYS_THRESHOLD = 90
BLOOMBERG_DATES_HEADER = "== Bloomberg dates =="
SYNTAX_INPUT_ERROR = "Input syntax not correct."


def launch(method_name, argument_list):
    try:
        command = globals()[method_name]
        return command(*argument_list)
    except KeyError:
        print(f"Method {method_name} not implemented")


db = Database(PRIVATE_DB_PATH)


def add_security(*args):
    try:
        name, ticker, country, ir_website, currency = args
    except ValueError:
        print(f"{SYNTAX_INPUT_ERROR} Example: {Security.example_input()}")
        return

    db.insert_security(Security(" ".join(name.split("-")), ticker, country, ir_website, currency))


def add_earnings_date(*args):
    try:
        ticker, swiss_date = args
    except ValueError:
        print(f"{SYNTAX_INPUT_ERROR} Example: {EarningsDate.example_input()}")
        return

    db.insert_earnings_date(EarningsDate(ticker, swiss_date))


def add_list_change(*args):
    try:
        ticker, pref_list, event, event_swiss_date = args
    except ValueError:
        print(f"{SYNTAX_INPUT_ERROR} Example: {ListChange.example_input()}")
        return

    note = " ".join(args[4:]) if len(args) > 4 else None
    db.insert_list_change(ListChange(ticker, pref_list, event, event_swiss_date, note))


def query_list_components(pref_list):
    try:
        list_info = db.query_list_info(pref_list)
    except Exception as e:
        print(f"Could not find list: Reason {e}")
        return

    list_components = db.query_list_components(pref_list)
    print(f"== {list_info.name} (weight {list_info.weight} / parent {list_info.parent_list_weight}) ==")
    for component in list_components:
        print(f"[added {timestamp_to_swiss_date(component.latest_change_timestamp)}] "
              f"{Security.summary(component.name, component.ticker)}: "
              f"{db.print_dates_summary(component.ticker)}")


def query_security(name):
    for security in db.query_security_info(name):
        print(f"{Security.summary(security.ticker, security.name)}: {security.ir_website}")


def query_list_of_lists(parent_pref_list):
    for pref_list in db.query_list_of_lists(parent_pref_list):
        query_list_components(pref_list)


def query_points(*args):
    threshold_days = int(args[0]) if args else POINTS_DAYS_THRESHOLD

    points_results = db.query_points(threshold_days)

    for result in points_results:
        if result.aggregate_points_value > 0:
            print(f"{result.aggregate_points_value}: {Security.summary(result.name, result.ticker)} "
                  f"{db.print_dates_summary(result.ticker)}")


def query_time_weighted_points(*args):
    threshold_days = int(args[0]) if args else POINTS_DAYS_THRESHOLD

    aggregated_values = process_weighted_points_results(*db.query_time_weighted_points(threshold_days))

    aggregated_values.sort(reverse=True)
    for value in aggregated_values:
        print(f"{value} {db.print_dates_summary(value.ticker)}")


def process_weighted_points_results(twp_results, factor_calculator):
    aggregated_values = []

    processor = None
    current_result = None
    for result in twp_results:
        if processor is None:
            pass
        elif result.ticker == processor.ticker:
            processor.update_current_points(result)
            continue
        elif processor.points > 0:
            aggregated_values.append(AggregatedValue(processor.points, current_result))

        processor = WeightedPointsProcessor(result, factor_calculator)
        current_result = result

    if processor is not None and processor.points > 0:
        aggregated_values.append(AggregatedValue(processor.points, current_result))

    return aggregated_values


def query_list_history(pref_list):
    for history_event in db.query_list_history(pref_list):
        print(f"{Security.summary(history_event.name, history_event.ticker)}: {history_event.event_name} "
              f"[{timestamp_to_swiss_date(history_event.event_date_timestamp)}]")


def query_earnings_for_ticker(ticker):
    print(f"{print_earnings_for_ticker(ticker)}")


def print_earnings_for_ticker(ticker):
    result = ""
    for date in db.query_native_earnings_dates(ticker):
        result += f"\n{timestamp_to_swiss_date(date)}"

    result += f"\n{BLOOMBERG_DATES_HEADER}"
    for date in db.query_bloomberg_earnings_dates(ticker):
        result += f"\n{timestamp_to_swiss_date(date)}"

    return result


def query_ticker(ticker):
    try:
        security = db.query_security(ticker)
    except StopIteration:
        print(f"No security found for ticker {ticker}")
        return

    histories = db.query_history(ticker)

    print(f"Name: {security.name}\nCountry: {security.country} ({security.country_weight})"
          f"\nCurrency: {security.currency} ({security.currency_weight})\nIR: {security.ir_website}"
          f"\nPoints: {print_points_summary_for_ticker(ticker)})")
    print(f"\n\n### List History ###{print_histories(histories)}")
    print(f"\n### Earnings History ###{print_earnings_for_ticker(ticker)}")


def print_points_summary_for_ticker(ticker):
    points_result = db.query_points(POINTS_DAYS_THRESHOLD, ticker=ticker)
    time_weighted_points_result = process_weighted_points_results(*db.query_time_weighted_points(POINTS_DAYS_THRESHOLD,
                                                                                                 ticker=ticker))

    # queried one ticker so expect zero or one results
    try:
        points = next(points_result).aggregate_points_value
    except StopIteration:
        points = 0
    time_weighted_points = 0.00 if len(time_weighted_points_result) == 0 else time_weighted_points_result[0].value

    return f"[T: {time_weighted_points:.2f}, P: {points}]"


def print_histories(histories):
    current_list_ticker = None
    result = ""

    for history_event in histories:
        if history_event.pref_list != current_list_ticker:
            current_list_ticker = history_event.pref_list
            current_list_info = db.query_list_info(current_list_ticker)
        result += f"\n{history_event.list_name} ({current_list_info.weight}/{current_list_info.parent_list_weight}): " \
                  f"{history_event.event_name} [{timestamp_to_swiss_date(history_event.event_date_timestamp)}] " \
                  f"{'' if history_event.event_note is None else history_event.event_note}"

    return result


def clean_up():
    db.close()
    raise SystemExit


def main():
    db.connect()
    menu = Menu(MENU_OPTIONS, launch)
    menu.wait_for_input()


if __name__ == "__main__":
    main()
