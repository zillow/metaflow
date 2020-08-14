from metaflow import FlowSpec, step

class ForeachFlow(FlowSpec):
    """
    A flow to demo manual, foreach orchestration (i.e.,
    by decoupling the step run from the orchestration for the special
    foreach case.
    """

    @step
    def start(self):
        print("This is a foreach node which will branch into an unknown number of nodes")
        import random

        # Let's generate a list of numbers. We do not know
        # length of this list until this piece of code executes.
        self.list_to_explore = [x for x in range(random.randint(2,4))]
        print("(For validation from inside the flow) Foreach fanout num_splits : ", len(self.list_to_explore))

        self.next(self.explore, foreach='list_to_explore')

    @step
    def explore(self):
        print("Inside explore...")
        print("Reading input variable: ", self.input)
        print("Done exploring...")

        self.next(self.join)

    @step
    def join(self, inputs):
        print("Inside join")
        self.next(self.end)

    @step
    def end(self):
        print("END: Flow complete!")


if __name__ == '__main__':
    ForeachFlow()