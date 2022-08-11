from metaflow.plugins.argo.argo_client import ArgoClient
from metaflow.metaflow_config import KUBERNETES_NAMESPACE
from metaflow.plugins.argo.argo_live_run import ArgoLiveRun
import time


class LiveRun:
    """
    This class takes in information to identify and trigger a run of a
    Metaflow flow using the Argo plugin. The run is initialized without
    triggering a run, but a run should be triggered immediately after
    initialization. This class should be initialized only with the trigger_run
    function. The flow to trigger is identified based on the parameter for the
    Metaflow flow name. Users may use additional parameters to alter the run,
    and wait for the run to finish.

    The object allows users to access information relating to the triggered
    run, including booleans for whether the run has been triggered, has
    finished, and was successful, as well as for failed steps, and exceptions.
    """

    def __init__(
        self,
        plugin_run,
    ):
        """
        Initialize a TriggeredRun object without triggering flow.

        Parameters
        ----------
        flow_name: str
            The name of the Metaflow flow name to trigger a run of
        parameters: dict
            The information passed in to affect how run is triggered
        wait: bool
            whether function waits for triggered run to finish before returning
        wait_timeout: int
            time (in mins) to wait for run to finish
        """

        # initialize instance variables
        self._plugin_run = plugin_run

    def trigger(self) -> None:
        self._plugin_run.trigger()

    @property
    def _status(self) -> str:
        # TODO: Consider deleting this since it's supposed to be private.
        # However, it's good for testing, so maybe keep.
        return self._plugin_run._status

    @property
    def has_triggered(self) -> bool:
        return self._plugin_run.has_triggered

    @property
    def finished(self) -> bool:
        return self._plugin_run.finished

    @property
    def successful(self) -> bool:
        return self._plugin_run.successful

    @property
    def failed_steps(self) -> list:
        return self._plugin_run.failed_steps

    @property
    def exceptions(self) -> dict:
        return self._plugin_run.exceptions


def trigger_run(
    plugin_name: str = 'Argo',
    flow_name: str = None,
    parameters: dict = None,
    wait: bool = True,
    wait_timeout: int = 30,  # in minutes
) -> LiveRun:
    """
    Triggers run of Metaflow flow and returns LiveRun object.

    During initialization, this class immediately triggers a run of a
    Metaflow flow using the Argo plugin. The class also provides access to
    information relating to the run.

    Parameters
    ----------
    flow_name: str
        The name of the Metaflow flow name to trigger a run of
    parameters: dict
        The information passed in to affect how run is triggered
    wait: bool
        whether function waits for triggered run to finish before returning
    wait_timeout: int
        time (in mins) to wait for run to finish
    """
    plugin_trigger_functions = {
        'Argo': ArgoLiveRun
    }
    if plugin_name not in plugin_trigger_functions:
        raise Exception("plugin not found or not specified")

    plugin_class = plugin_trigger_functions[plugin_name]
    plugin_run = plugin_class(
        flow_name,
        parameters,
        wait,
        wait_timeout,
    )

    run = LiveRun(plugin_run)
    run.trigger()

    return run
