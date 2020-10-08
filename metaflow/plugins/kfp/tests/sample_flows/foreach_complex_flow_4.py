from metaflow import FlowSpec, step


class ForeachComplexFlow4(FlowSpec):
    """
    Flow which contains 2 foreach branches (foreach1 and foreach2) nested in 2
    separate static branches (br11 and br12). The foreach branches are separately
    joined in join1 and join2, which are in turn joined by join3.
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
        self.next(self.foreach1)

    @step
    def br12(self):
        self.var1 = 200
        self.next(self.foreach2)

    @step
    def foreach1(self):
        import random

        self.list_to_explore = [x for x in range(1)]
        self.next(self.explore, foreach="list_to_explore")

    @step
    def explore(self):
        self.val = "%s processed" % self.input
        self.next(self.join1)

    @step
    def foreach2(self):
        import random

        self.list_to_explore = [x for x in range(2)]
        self.next(self.explore2, foreach="list_to_explore")

    @step
    def explore2(self):
        self.val = "%s processed" % self.input
        self.next(self.join2)

    @step
    def join1(self, inputs):
        self.results = [input.val for input in inputs]
        self.next(self.join3)

    @step
    def join2(self, inputs):
        self.results = [input.val for input in inputs]
        self.next(self.join3)

    @step
    def join3(self, br1_inp):
        assert len(br1_inp.join1.results) == 1
        assert len(br1_inp.join2.results) == 2
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachComplexFlow4()
