from metaflow.plugins.argo.argo_live_run import ArgoLiveRun


def trigger_live_run(
    plugin_name: str = "Argo",  # TODO: change default to None after testing
    flow_name: str = None,
    alt_flow_id_info: dict = None,
    parameters: dict = None,
    wait: bool = True,
    wait_timeout: int = 30,  # in minutes
):  # -> LiveRun
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
        Name of Metaflow flow to trigger run
    alt_flow_id_info: dict
        Alternative identifying information of flow to trigger run
    parameters: dict
        The information passed in to affect how run is triggered
    wait: bool
        whether function waits for triggered run to complete before returning
    wait_timeout: int
        time (in mins) to wait for run to complete
    """
    if alt_flow_id_info is None:
        alt_flow_id_info = {}

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

    return live_run_class.trigger_live_run(
        flow_name,
        alt_flow_id_info,
        parameters,
        wait,
        wait_timeout,
    )
