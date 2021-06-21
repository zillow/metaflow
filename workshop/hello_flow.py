
from metaflow import FlowSpec, step


class HelloFlow(FlowSpec):
    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """
        self.next(self.hello)

    @step
    def hello(self):
        print("Hello <TODO>.")
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        print("HelloFlow is all done.")


if __name__ == "__main__":
    HelloFlow()
