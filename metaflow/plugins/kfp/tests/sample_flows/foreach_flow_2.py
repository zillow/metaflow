from metaflow import FlowSpec, step


class ForeachFlow2(FlowSpec):
    @step
    def start(self):
        import random

        num_splits = random.randint(2, 4)
        self.list_to_explore = [x for x in range(num_splits)]
        self.next(self.explore, foreach="list_to_explore")

    @step
    def explore(self):
        self.val = "%s processed" % self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [input.val for input in inputs]
        self.next(self.end)

    @step
    def end(self):  # type: end node
        pass


if __name__ == "__main__":
    ForeachFlow2()
