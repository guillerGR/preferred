from date_util import swiss_date_to_timestamp


class ListChange:
    def __init__(self, security_ticker, pref_list, event, swiss_date, note):
        self.security_ticker = security_ticker.strip()
        self.pref_list = pref_list.strip()
        self.event = event.strip()
        self.timestamp = swiss_date_to_timestamp(swiss_date.strip())
        self.note = note

    @staticmethod
    def example_input():
        return "APD.US US add 30.06.19"