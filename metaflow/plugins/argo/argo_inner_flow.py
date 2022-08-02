from metaflow.plugins.argo.argo_client import ArgoClient
from metaflow.metaflow_config import KUBERNETES_NAMESPACE
import time

### ADDING WAIT FEATURE AND UPDATING PROPERTIES


class TriggeredRun:
    def __init__(
        self,
        flow_name: str = None,
        parameters: dict = None,
        wait: bool = True,
        wait_to_trigger: int = 100,  # In minutes (REMEMBER TO CHANGE IT TO MINUTES FOR ACTUAL VERSION)
        wait_to_run: int = 30,  # In minutes (REMEMBER TO CHANGE IT TO MINUTES FOR ACTUAL VERSION)
    ):
        """Initializes and triggers a run.

        Keyword arguments:
        flow_name -- Metaflow flow name to trigger
        parameters -- information passed in to affect how run is triggered
        wait -- whether function waits for triggered run to finish before returning
        wait_to_trigger -- time (in mins) to wait for run to be triggered
        wait_to_run -- time (in mins) to wait for run to finish
        """
        if parameters is None:
            parameters = {}

        # initialize instance variables
        self._flow_name = flow_name
        self._template_name = flow_name.lower()
        self._parameters = parameters
        self._wait = wait
        self._wait_to_trigger = wait_to_trigger
        self._wait_to_run = wait_to_run
        self._argo_client = ArgoClient(KUBERNETES_NAMESPACE)
        self._exception = None
        self._status = None

        # trigger flow and retrieve id info
        self._flow_information = self._argo_client.trigger_workflow_template(
            self._template_name,
            parameters=self._parameters,
        )

        self._argo_run_id = self._flow_information["metadata"]["name"]
        self._metaflow_run_id = f"argo-{self._argo_run_id}"
        self._kubernetes_namespace = self._flow_information["metadata"]["namespace"]

        # Waiting for Argo workflow to trigger is not optional.
        # It should happen quickly, and is necessary to determine Flow name if not given
        while wait_to_trigger > 0 and self.status is None:
            print("not ready yet - give me 1 more second")
            time.sleep(1)
            wait_to_trigger -= 1

        if wait_to_trigger == 0:
            raise Exception("Inner flow failed to begin running")

        print(
            f"A run of Metaflow flow {self._flow_name} has started w/ Argo id: {self._argo_run_id}"
        )

        # [optional] wait for argo workflow to finish running
        if self._wait:
            print("\nNow we will wait for the flow to finish")
            wait_remaining = self._wait_to_run  # in minutes (5s for testing)
            print(f"status: {self.status}, 'mins' to wait: {wait_remaining}")
            while wait_remaining > 0 and self.status == "Running":
                time.sleep(5)
                wait_remaining -= 1
                print(f"status: {self.status}, 'mins' to wait: {wait_remaining}")

            if wait_remaining == 0:
                print("Inner flow timed out. better work on that speed for next time!")

            print("\nInner flow is finished!!!")

        else:
            print("Not waiting for inner flow to finish")

    @property
    def status(self):
        wf = self._argo_client.get_workflow(self._inner_run_argo_id)

        if "status" in wf and "phase" in wf["status"]:
            self._status = wf["status"]["phase"]

        return self._status

    @property
    def failed_steps(self):
        if self.status != "Failed":
            return []

        failed_steps = []

        wf = self._argo_client.get_workflow(self._inner_run_argo_id)
        nodes_info = wf["status"]["nodes"]
        for node in nodes_info:
            if (
                nodes_info[node]["phase"] == "Failed"
                and nodes_info[node]["type"] == "Pod"
            ):
                print(f"{nodes_info[node]['templateName']} step failed")
                failed_steps.append(nodes_info[node]["templateName"])

        self._failed_steps = failed_steps

        return self._failed_steps

    @property
    def exception(self):
        if len(self.failed_steps) == 0:
            return None
        print(self.failed_steps)
        # How do I get the exception info from Argo Client?
        # Do I have to go back to Metaflow's Run or Step class?
        # I think I'd like to stay w/ Argo Client
        # Argo logs and/or Argo watch work in CLI. Is there a way to get that info through ArgoClient?
