from metaflow import FlowSpec, step
from metaflow.client.trigger_live_run import trigger_live_run
import time
import sys
import subprocess


subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'kubernetes'])


class LiveRunFlowTriggeringFlow(FlowSpec):
    """
    A flow that triggers another flow.
    """

    @step
    def start(self):
        """
        This step waits 5 seconds to let the trigger function test status.
        """
        print("LiveRunFlowTriggeringFlow is starting")
        time.sleep(5)
        self.next(self.trigger_step)

    @step
    def trigger_step(self):
        """
        This step triggers another flow.
        """
        print("LiveRunFlowTriggeringFlow is in its trigger_step")

        # first trigger
        run_one = trigger_live_run(
            plugin_name="Argo",
            flow_name="LiveRunSimpleFlow",
            template_name=None,
            parameters=None,
            wait=False,
        )
        assert run_one.has_triggered is True
        assert run_one.is_running is True
        assert run_one.successful is False

        # second trigger
        run_two = trigger_live_run(
            plugin_name="Argo",
            flow_name="LiveRunSimpleFlow",
            template_name=None,
            parameters=None,
            wait=True,
        )
        assert run_two.has_triggered is True
        assert run_two.is_running is False
        assert run_two.successful is True

        # wait for first flow to complete
        while run_one.is_running:
            time.sleep(5)

        # re-test first flow properties
        assert run_one.has_triggered is True
        assert run_one.is_running is False
        assert run_one.successful is True

        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step.
        """
        print("LiveRunFlowTriggeringFlow is all done.")


if __name__ == "__main__":
    LiveRunFlowTriggeringFlow()
