from metaflow.client.trigger_live_run import trigger_live_run  # trigger
import time

start_time = time.time()
flow_name = "LiveRunSimpleFlow"
# "LiveRunSimpleFlow", "LiveRunParamFlow", "LiveRunFailureFlow",
# "LiveRunMissingFlow", "LiveRunFlowTriggeringFlow", None
template_name = None
# "liverunsimpleflow", "liverunparamflow", "liverunfailureflow",
# "liverunmissingflow", "liverunflowtriggeringflow", None

run = trigger_live_run(
    plugin_name="Argo",
    flow_name=flow_name,
    template_name=template_name,
    parameters={"sample_param": "sample_val"},
    wait=False,
    wait_timeout=60,  # in minutes
)

print(f"\nRunning flow {run.flow_name}")
while run.is_running:
    print(
        f"Status (Argo-only): '{run._status}' after {int(time.time()-start_time)} seconds"
    )
    run._print_status()
    time.sleep(10 - (time.time() - start_time) % 10)

print(
    f"Final status (Argo-only): '{run._status}' after {int(time.time()-start_time)} seconds"
)
run._print_status()
