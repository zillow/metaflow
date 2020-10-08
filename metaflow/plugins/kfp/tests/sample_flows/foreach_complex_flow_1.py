from metaflow import FlowSpec, step


class ForeachComplexFlow1(FlowSpec):
    """
    This flow contains a static branch br1 which is joined by join1 and followed
    by a foreach branch foreach1 which is in turn joined by join2.
    """

    @step
    def start(self):
        self.next(self.br1)

    @step
    def br1(self):
        self.next(self.br11, self.br12)

    @step
    def br11(self):
        self.next(self.join1)

    @step
    def br12(self):
        self.next(self.join1)

    @step
    def join1(self, br1_inp):
        self.next(self.linear1)

    @step
    def linear1(self):
        self.next(self.foreach1)

    @step
    def foreach1(self):
        import random

        num_splits = random.randint(2, 4)
        # Let's generate a list of numbers. We do not know
        # length of this list until this piece of code executes.
        self.list_to_explore = [x for x in range(num_splits)]
        print(
            "(For validation from inside the flow) Foreach fanout num_splits : ",
            len(self.list_to_explore),
        )
        self.next(self.explore, foreach="list_to_explore")

    @step
    def explore(self):  # type: linear node
        self.val = "%s processed" % self.input
        self.next(self.join2)

    @step
    def join2(self, inputs):  # type: join node
        self.results = [input.val for input in inputs]
        print("Joining this list: ", self.results)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachComplexFlow1()
