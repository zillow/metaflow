from metaflow import FlowSpec, step


class LinearFlow1(FlowSpec):
    @step
    def start(self):
        self.x = 100
        self.next(self.hello)

    @step
    def hello(self):
        assert self.x == 100
        assert self.input is None
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    LinearFlow1()
