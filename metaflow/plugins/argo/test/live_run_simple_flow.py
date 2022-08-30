from metaflow import FlowSpec, step
import time


class LiveRunSimpleFlow(FlowSpec):
    """
    A simple flow for testing ability to trigger a flow via a function.
    """

    @step
    def start(self):
        """
        This step waits 5 seconds to let the trigger function test status.
        """
        print("LiveRunSimpleFlow is starting")
        time.sleep(5)
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step.
        """
        print("LiveRunSimpleFlow is all done.")


if __name__ == "__main__":
    LiveRunSimpleFlow()
