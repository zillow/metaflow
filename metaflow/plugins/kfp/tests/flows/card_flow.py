from metaflow import FlowSpec, Parameter, step, card
from datetime import datetime


class CardFlow(FlowSpec):

    alpha = Parameter("alpha", default=0.5)

    @card
    @step
    def start(self):
        """Generating default card"""
        self.example_dict = {"first_key": list(range(10)), "second_key": {"one", "two"}}
        self.timestamp = datetime.utcnow()
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    CardFlow()
