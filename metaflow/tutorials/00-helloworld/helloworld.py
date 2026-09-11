from metaflow import FlowSpec, step, resources, exit_handler, Parameter, JSONType

from metaflow.plugins.aip import exit_handler_resources, exit_handler_retry


@exit_handler_retry(times=1)
# @exit_handler_resources(cpu="600m", memory="600Mi")
def my_exit_handler(
    status: str,
    flow_parameters: dict,
    argo_workflow_run_name: str,
    metaflow_run_id: str,
    argo_ui_url: str,
    retries: int,
    failures: str,
) -> None:
    print(f"{failures=}")
    print(f"{flow_parameters=}")
    if retries == 0:
        raise Exception("oopsie")


@exit_handler(func=my_exit_handler, on_success=True)
class HelloFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """

    s3_keys = Parameter("s3_keys", type=JSONType)
    hi = Parameter("hi", default=None, help="The word to print.")
    hi2 = Parameter("hi2")

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        self.x = 42
        self.y = {"foo": "bar", "baz": "qux", "quux": "corge"}
        # raise Exception("foo")
        print("HelloFlow is starting.")
        # import time
        # time.sleep(3)
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
