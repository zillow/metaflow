from metaflow.plugins.argo import trigger_live_run
parameters = None # {"date_key": "wrong_date"}

inner_run = trigger_live_run(
    flow_name="HelloArgoFlowTwo",     # "HelloArgoFlowTwo", "FailureFlow", "MissingFlow"
    # template_name="helloargoflowtwo", # "helloargoflowtwo", "failureflow"
    parameters=None,
    wait=True,
)
