from metaflow.plugins.argo import tester  # trigger_live_run
parameters = None # {"date_key": "wrong_date"}

print("ok here we go")

tester()

"""
inner_run = trigger_live_run(
    flow_name="HelloArgoFlowTwo",     # "HelloArgoFlowTwo", "FailureFlow", "MissingFlow"
    # template_name="helloargoflowtwo", # "helloargoflowtwo", "failureflow"
    parameters=None,
    wait=True,
)
"""
