from metaflow import FlowSpec, step


class ForeachComplexFlow2(FlowSpec):
    """
    A flow which contains 2 successive foreach branches followed by 1 static branch.
    """

    @step
    def start(self):
        self.next(self.foreach1)

    @step
    def foreach1(self):
        import random

        num_splits = random.randint(2, 4)
        self.list_to_explore = [x for x in range(num_splits)]
        self.next(self.explore, foreach="list_to_explore")

    @step
    def explore(self):
        self.val = "%s processed" % self.input
        self.next(self.join1)

    @step
    def join1(self, inputs):
        self.results = [input.val for input in inputs]
        self.next(self.linear1)

    @step
    def linear1(self):
        print(self.results)
        self.next(self.foreach2)

    @step
    def foreach2(self):
        import random

        num_splits = random.randint(2, 4)
        self.list_to_explore2 = [x for x in range(num_splits)]
        self.next(self.explore2, foreach="list_to_explore2")

    @step
    def explore2(self):
        self.val = "%s processed" % self.input
        self.next(self.join2)

    @step
    def join2(self, inputs):
        self.results2 = [input.val for input in inputs]
        self.next(self.linear2)

    @step
    def linear2(self):
        self.next(self.br1)

    @step
    def br1(self):
        self.next(self.br11, self.br12)

    @step
    def br11(self):
        self.var1 = 100
        self.next(self.join3)

    @step
    def br12(self):
        self.var1 = 200
        self.next(self.join3)

    @step
    def join3(self, br1_inp):
        print(br1_inp.br11.var1)
        print(br1_inp.br12.var1)
        self.next(self.end)

    @step
    def end(self):  # type: end node
        pass


if __name__ == "__main__":
    ForeachComplexFlow2()
