from metaflow.plugins.argo.argo_client import ArgoClient
from metaflow.metaflow_config import KUBERNETES_NAMESPACE
import time

### ADDING WAIT FEATURE AND UPDATING PROPERTIES


class TriggeredRun:
    """
    This class takes in information to identify and trigger a run of a
    Metaflow flow using the Argo plugin. The run is triggered immediately 
    upon initialization of this object, and the run is identified based on 
    the parameter for the Metaflow flow name. Users may use additional 
    parameters to alter the run, as well as whether to wait for the run to 
    finish. 
    
    The object allows users to access information relating to the triggered 
    run, including status ('Running', 'Failed', or 'Successful'), failed
    steps, and exceptions.
    """
    
    def __init__(
        self,
        flow_name: str = None,
        parameters: dict = None,
        wait: bool = True,
        wait_to_run: int = 30,  # in minutes (REMEMBER TO CHANGE IT TO MINUTES)
    ):
        """
        Initialize a TriggeredRun Metaflow run.
        
        During initialization, this class immediately triggers a run of a
        Metaflow flow using the Argo plugin. The class also provides access to
        information relating to the run.

        Parameters
        ----------
        flow_name: str
            The name of the Metaflow flow name to trigger
        parameters: dict
            The information passed in to affect how run is triggered
        wait: bool
            whether function waits for triggered run to finish before returning
        wait_to_run: int
            time (in mins) to wait for run to finish
        """        
        if parameters is None:
            parameters = {}

        # initialize instance variables
        self._flow_name = flow_name
        self._template_name = flow_name.lower()
        self._parameters = parameters
        self._wait = wait
        self._wait_to_run = wait_to_run
        self._argo_client = ArgoClient(KUBERNETES_NAMESPACE)
        self._exception = None
        self._status = None
        self._metaflow_run = None

        # trigger flow and retrieve id info
        self._flow_information = self._argo_client.trigger_workflow_template(
            self._template_name,
            parameters=self._parameters,
        )

        self._argo_run_id = self._flow_information["metadata"]["name"]
        self._metaflow_run_id = f"argo-{self._argo_run_id}"
        self._kubernetes_namespace = self._flow_information["metadata"]["namespace"]

        # Waiting for Argo workflow to trigger is not optional.
        # It is necessary to determine Flow name if not given as parameter
        wait_to_trigger = 20  # wait time is 20 seconds
        print("attempting to trigger Argo workflow")
        while wait_to_trigger > 0 and self.status is None:
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
                wait_remaining -= 1
                print(f"status: {self.status}, 'mins' to wait: {wait_remaining}")
                time.sleep(5)

            if wait_remaining == 0:
                print("Inner flow timed out. better work on that speed for next time!")

            print("\nInner flow is finished!!!")

        else:
            print("Not waiting for inner flow to finish")

            
    @property
    def status(self):
        wf = self._argo_client.get_workflow(self._argo_run_id)

        if "status" in wf and "phase" in wf["status"]:
            self._status = wf["status"]["phase"]

        return self._status

    
    @property
    def failed_steps(self):
        if self.status != "Failed":
            return []

        failed_steps = []

        wf = self._argo_client.get_workflow(self._argo_run_id)
        nodes_info = wf["status"]["nodes"]
        for node in nodes_info:
            if (
                nodes_info[node]["phase"] == "Failed"
                and nodes_info[node]["type"] == "Pod"
            ):
                failed_steps.append(nodes_info[node]["templateName"])

        self._failed_steps = failed_steps

        return self._failed_steps

    
    def _find_metaflow_run(self, metaflow_run_location):
        from metaflow import Run, namespace
        
        namespace(None)
        metaflow_run_location = self._flow_name + "/" + self._metaflow_run_id
        attempts_to_find_metaflow_run = 60  # gives 1 minute to find metaflow run
        print("Finding Metaflow run")
        while attempts_to_find_metaflow_run > 0:
            try:
                metaflow_run = Run(metaflow_run_location)
                self._metaflow_run = metaflow_run
                break
            except:
                time.sleep(1)
                attempts_to_find_metaflow_run -= 1

    
    @property
    def exceptions(self):
        if self._metaflow_run == None:
            self._find_metaflow_run(self)
            if self._metaflow_run == None:
                raise Exception("Could not find Metaflow run")
        
        exceptions = {}
        
        for step in self._metaflow_run.steps():
            for task in step.tasks():
                if task.exception is not None:
                    if str(step) not in exceptions:
                        exceptions[str(step)] = [{'step': step, 'exceptions': [{'task': task, 'exception': task.exception}]}]
                    else:
                        exceptions[str(step)]['exceptions'].append({'task': task, 'exception': task.exception})

        return exceptions
