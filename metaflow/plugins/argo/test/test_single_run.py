from metaflow.client.trigger_live_run import trigger_live_run  # trigger
import time

start_time = time.time()
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
)

print(f"\nRunning flow {run.flow_name}")
while run.is_running:
    print(
        f"Status (Argo-only): '{run._status}' after {int(time.time()-start_time)} seconds"
    )
    run._print_status()
    time.sleep(15 - (time.time() - start_time) % 15)

print(
    f"Final status (Argo-only): '{run._status}' after {int(time.time()-start_time)} seconds"
)
run._print_status()
