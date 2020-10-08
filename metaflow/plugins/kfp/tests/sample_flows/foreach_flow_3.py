from metaflow import FlowSpec, step


class ForeachFlow3(FlowSpec):
    """
    This follow contains 2 successive linear fanout steps, explore and explore2.
    """

    @step
    def start(self):
        import random

        num_splits = random.randint(2, 4)
        self.list_to_explore = [x for x in range(num_splits)]
        self.next(self.explore, foreach="list_to_explore")

    @step
    def explore(self):
        self.my_input = self.input
        self.my_input = self.my_input + 1
        self.next(self.explore2)

    @step
    def explore2(self):
        self.my_input = self.my_input + 1
        self.val = self.my_input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [input.val for input in inputs]
        # ensure we modified the input to this step
        # using self.my_input twice
        assert 0 not in self.results
        assert 1 not in self.results
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachFlow3()
