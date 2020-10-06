from metaflow import FlowSpec, step


class ComplexFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.br1, self.br2)

    @step
    def br1(self):
        self.next(self.br11, self.br12)

    @step
    def br2(self):
        self.next(self.br21, self.br22)

    @step
    def br11(self):
        self.next(self.join1)

    @step
    def br12(self):
        self.next(self.join1)

    @step
    def br21(self):
        self.next(self.join2)

    @step
    def br22(self):
        self.next(self.join2)

    @step
    def join1(self, br1_inp):
        self.var1 = 200
        self.next(self.join3)

    @step
    def join2(self, br2_inp):
        self.var2 = 100
        self.next(self.join3)

    @step
    def join3(self, br_inp):
        assert br_inp.join1.var1 == 200
        assert br_inp.join2.var2 == 100
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ComplexFlow()
