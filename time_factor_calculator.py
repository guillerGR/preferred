class TimeFactorCalculator:
    def __init__(self, current_time, max_time_diff):
        self.current_time = current_time
        self.max_time_diff = max_time_diff

    def calculate_for_change_time(self, change_time):
        time_diff = (self.current_time - change_time)
        if time_diff > self.max_time_diff or time_diff < 0:
            raise Exception(f"Unexpected change time {change_time}")
        return float(1 - float(time_diff) / float(self.max_time_diff))