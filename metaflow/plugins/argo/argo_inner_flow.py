# from metaflow import Run, namespace
from metaflow.plugins.argo.argo_client import ArgoClient
import time


class TriggeredRun:
    def __init__(
        self,
        flow_name: str = None,
        parameters: dict = None,
        wait_to_trigger: int = 100,  # In minutes (REMEMBER TO CHANGE IT TO MINUTES FOR ACTUAL VERSION b/c testing uses shorter times)
    ):
        from metaflow import (
            Run,
            namespace,
        )  # consider cleaning this up so that import is on top

        # current issue is that import on top creates circular error
        self._flow_name = flow_name
        self._template_name = flow_name.lower()

        if parameters is None:
            parameters = {}
        self._parameters = parameters

        self._argo_client = ArgoClient("aip-example-sandbox")

        self._flow_information = self._argo_client.trigger_workflow_template(
            self._template_name,
            parameters=self._parameters,
        )

        self._inner_run_argo_id = self._flow_information["metadata"]["name"]
        self._inner_run_metaflow_id = "argo-" + self._inner_run_argo_id
        self._inner_namespace = self._flow_information["metadata"]["namespace"]
        self._exception = None
        self._status = None

        print(f"Inner flow has started w/ Argo id: {self._inner_run_argo_id}")
