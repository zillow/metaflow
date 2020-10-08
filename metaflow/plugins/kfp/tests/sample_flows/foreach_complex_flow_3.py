from metaflow import FlowSpec, step


class ForeachComplexFlow3(FlowSpec):
    """
    This flow contains a foreach branch nested within a static branch.
    The foreach is joined into a list within br12 and collected again in join2.
    """

    @step
    def start(self):
        self.next(self.br1)

    @step
    def br1(self):
        self.next(self.br11, self.br12)

    @step
    def br11(self):
        self.var1 = 100
        self.next(self.join2)

    @step
    def br12(self):
        self.next(self.foreach1)

    @step
    def foreach1(self):
        import random

        self.num_splits = 2
        self.list_to_explore = [x for x in range(self.num_splits)]
        print(
            "(For validation from inside the flow) Foreach fanout num_splits : ",
            len(self.list_to_explore),
        )
        self.next(self.explore, foreach="list_to_explore")

    @step
    def explore(self):
        self.val = "%s processed" % self.input
        self.next(self.join1)

    @step
    def join1(self, inputs):
        self.results = [input.val for input in inputs]
        assert len(self.results) == 2  # num_splits
        self.next(self.join2)

    @step
    def join2(self, br1_inp):
        assert len(br1_inp.join1.results) == 2  # num_splits
        assert br1_inp.br11.var1 == 100
        self.next(self.end)

    @step
    def end(self):  # type: end node
        pass


if __name__ == "__main__":
    ForeachComplexFlow3()
