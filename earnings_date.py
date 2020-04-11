from date_util import swiss_date_to_timestamp


class EarningsDate:
    def __init__(self, security_ticker, date):
        self.security = security_ticker.strip()
        self.timestamp = swiss_date_to_timestamp(date.strip())

    @staticmethod
    def example_input():
        return "APD.US 30.06.19"
