from metaflow.plugins.argo.argo_live_run import ArgoLiveRun
from .live_run import LiveRun


def trigger_live_run(
    plugin_name: str = "Argo",  # TODO: change default to None after testing
    flow_name: str = None,
    parameters: dict = None,
    wait: bool = True,
    wait_timeout: int = 30,  # in minutes
    **kwargs,
) -> LiveRun:
    """
    Triggers run of Metaflow flow and returns an object in the LiveRun family.

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
        Name of Metaflow flow to trigger run
    parameters: dict
        The information passed in to affect how run is triggered
    wait: bool
        whether function waits for triggered run to complete before returning
    wait_timeout: int
        time (in mins) to wait for run to complete
    """

    plugin_live_run_classes = {
        "Argo": ArgoLiveRun
        # Other plugins (like KFP) can be added over time
    }
    if plugin_name not in plugin_live_run_classes:
        raise ValueError(
            f"""
    plugin_name '{plugin_name}' is not supported
    Supported plugins include: {[key for key in plugin_live_run_classes]}
        """
        )

    live_run_class = plugin_live_run_classes[plugin_name]

    return live_run_class.trigger(
        flow_name=flow_name,
        parameters=parameters,
        wait=wait,
        wait_timeout=wait_timeout,
        **kwargs,
    )
