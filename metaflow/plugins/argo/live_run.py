from metaflow.plugins.argo.argo_live_run import ArgoLiveRun


class LiveRun:
    """
    This class takes in an object representing a live run from a plugin
    ("[Plugin]LiveRun"). This class maps the functions and properties from the
    [Plugin]LiveRun object onto its own functions and properties, including
    the ability to trigger the run.

    This object allows users to access information relating to the run it
    represents, including booleans for whether the run has been triggered, has
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
        return self._plugin_run.trigger()

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


def trigger_live_run(
    plugin_name: str = 'Argo',  # TODO: change default to None after testing
    flow_name: str = None,
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
        # Other plugins (like KFP) can be added over time
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
