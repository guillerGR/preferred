from security import Security


class WeightedPointsProcessor:
    def __init__(self, twp_result, time_calculator):
        self.time_calculator = time_calculator
        self.points = self.get_current_points(twp_result)
        self.ticker = twp_result.ticker

    def get_current_points(self, twp_result):
        return twp_result.event_value * self.time_calculator.calculate_for_change_time(twp_result.event_date_timestamp)

    def update_current_points(self, update_twp_result):
        self.points += self.get_current_points(update_twp_result)


class AggregatedValue:
    def __init__(self, value, twp_result):
        self.value = value
        self.name = twp_result.name
        self.ticker = twp_result.ticker

    def __repr__(self):
        return f"{self.value:.2f}: {Security.summary(self.name, self.ticker)}"

    def __eq__(self, other):
        return self.value == other.value

    def __hash__(self):
        return hash(self.value)

    def __lt__(self, other):
        return self.value < other.value
