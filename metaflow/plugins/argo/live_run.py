from metaflow.plugins.argo.argo_live_run import ArgoLiveRun


class LiveRun:
    """
    This class takes in an object representing a live run from a plugin
    ("[Plugin]LiveRun"). This class maps the functions and properties from the
    [Plugin]LiveRun object onto its own functions and properties, including
    the ability to trigger the run.

    This object allows users to access information relating to the run it
    represents, including booleans for whether the run has been triggered, is
    running, and was successful, as well as for failed steps, and exceptions.
    """

    def __init__(
        self,
        plugin_run,
    ):
        """
        Initialize a TriggeredRun object without triggering flow.

        Parameters
        ----------
        plugin_run: [plugin]LiveRun object
            A object representing a live run of a particular plugin

        """

        # initialize instance variables
        self._plugin_run = plugin_run

    def trigger(self) -> None:
        return self._plugin_run.trigger()

    @property
    def has_triggered(self) -> bool:
        return self._plugin_run.has_triggered

    @property
    def is_running(self) -> bool:
        return self._plugin_run.is_running

    @property
    def successful(self) -> bool:
        return self._plugin_run.successful

    @property
    def failed_steps(self) -> list:
        return self._plugin_run.failed_steps

    @property
    def exceptions(self) -> dict:
        return self._plugin_run.exceptions


def trigger_live_run(
    plugin_name: str = 'Argo',  # TODO: change default to None after testing
    flow_id_info: dict = None,
    parameters: dict = None,
    wait: bool = True,
    wait_timeout: int = 30,  # in minutes
) -> LiveRun:
    """
    Triggers run of Metaflow flow and returns LiveRun object.

    This function takes in the name of a plugin and uses that plugin to create
    a [Plugin]LiveRun object. Then, a generic LiveRun object is created and the
    functionality of the [Plugin]LiveRun object is mapped to the generic
    LiveRun object.

    The function then triggers the LiveRun object and returns the object.

    Parameters
    ----------
    plugin_name: str
        The name of the plugin used to trigger this run
    flow_id_info: dict
        The identifying information of the Metaflow flow name to trigger run
    parameters: dict
        The information passed in to affect how run is triggered
    wait: bool
        whether function waits for triggered run to complete before returning
    wait_timeout: int
        time (in mins) to wait for run to complete
    """
    if flow_id_info is None:
        flow_id_info = {}

    plugin_live_run_classes = {
        'Argo': ArgoLiveRun
        # Other plugins (like KFP) can be added over time
    }
    if plugin_name not in plugin_live_run_classes:
        raise Exception("plugin either not found or not specified")

    plugin_class = plugin_live_run_classes[plugin_name]
    run = plugin_class(
        flow_id_info,
        parameters,
        wait,
        wait_timeout,
    )

    run.trigger()

    return run
