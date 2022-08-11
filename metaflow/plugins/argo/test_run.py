from metaflow.plugins.argo import trigger_live_run  # trigger_live_run


parameters = None  # {"date_key": "wrong_date"}

print("ok here we go")

inner_run = trigger_live_run(
    flow_id_info={"flow_name": "HelloArgoFlowTwo"},     # "HelloArgoFlowTwo", "FailureFlow", "MissingFlow"
    # template_name="helloargoflowtwo", # "helloargoflowtwo", "failureflow"
    parameters=None,
    wait=True,
)
