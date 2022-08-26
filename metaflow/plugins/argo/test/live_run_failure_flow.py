from metaflow import FlowSpec, step
import time


class LiveRunFailureFlow(FlowSpec):
    """
    This flow fails. Use this flow to test trigger_live_run function's handling
    of failure.
    """

    @step
    def start(self):
        """
        This step waits 5 seconds to let the trigger function test status.
        """
        print("LiveRunFailureFlow is starting")
        time.sleep(5)
        self.next(self.middle_step)

    @step
    def middle_step(self):
        """
        This step is a middle step and prints one line.
        """
        print("LiveRunFailureFlow is in its middle_step")
        self.next(self.failure_step)

    @step
    def failure_step(self):
        """
        This is the 'failure_step' step. This flow should fail at this step.
        """
        print("LiveRunFailureFlow is in its failure_step")
        raise Exception("LiveRunFailureFlow failed!")
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step.
        """
        print("LiveRunFailureFlow is all done.")


if __name__ == "__main__":
    LiveRunFailureFlow()
