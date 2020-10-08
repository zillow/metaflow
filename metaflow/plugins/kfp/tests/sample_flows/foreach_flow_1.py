from metaflow import FlowSpec, step


class ForeachFlow1(FlowSpec):
    @step
    def start(self):
        self.titles = ["Stranger Things", "House of Cards", "Narcos"]
        self.next(self.a, foreach="titles")

    @step
    def a(self):
        self.title = "%s" % self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [input.title for input in inputs]
        assert len(self.results) == 3
        assert "Stranger Things" in self.results
        assert "House of Cards" in self.results
        assert "Narcos" in self.results
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachFlow1()
