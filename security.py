class Security:
    def __init__(self, name, ticker, country, ir_website, currency):
        self.name = name.strip()
        self.ticker = ticker.strip()
        self.country = country.strip()
        self.currency = currency.strip()
        self.ir_website = ir_website.strip()

    @staticmethod
    def summary(name, ticker):
        return f"{name} ({ticker})"

    @staticmethod
    def example_input():
        return "Air-Products APD.US US http://investors.airproducts.com/upcoming-events USD"
