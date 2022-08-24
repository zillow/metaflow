from metaflow.client.trigger_live_run import trigger_live_run  # trigger
import time

flow_name = None  # "HelloArgoFlowTwo", "FailureFlow", "MissingFlow", None
template_name = (
    "helloargoflowtwo"  # "helloargoflowtwo", "failureflow", "missingflow", None
)
run = trigger_live_run(
    plugin_name="Argo",
    flow_name=flow_name,
    template_name=template_name,
    parameters=None,
    wait=False,
    x=2,
    y=55,
    z="zzzzz top",  # testing what happens when there are additional, unused kwargs
)

print(f"\nRunning flow {run.flow_name}")
start_time = time.time()
while run.is_running:
    print(
        f"Status (Argo-only): '{run._status}' after {int((time.time()-start_time)//1)} seconds"
    )
    run._print_status()
    time.sleep(15 - (time.time() - start_time) % 15)

print(
    f"Final status (Argo-only): '{run._status}' after {int((time.time()-start_time)//1)} seconds"
)
run._print_status()
