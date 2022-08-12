from metaflow.plugins.argo import trigger_live_run  # trigger_live_run
import time

flow_name = "HelloArgoFlowTwo"  # "HelloArgoFlowTwo", "FailureFlow", "MissingFlow"
run = trigger_live_run(
    flow_name=flow_name,
    parameters=None,
    wait=False,
)

print(f"\nRunning flow {flow_name}")
start_time = time.time()
while run.is_running:
    print(f"\nIn progress status after {int((time.time()-start_time)//1)} seconds:")
    print(f"Status (private): {run._plugin_run._status}")
    print(f"Has Triggered:    {run.has_triggered}")
    print(f"Is Running:       {run.is_running}")
    print(f"Successful:       {run.successful}")
    print(f"Failed Steps:     {run.failed_steps}")
    print(f"Exceptions:       {run.exceptions}\n")
    time.sleep(15)

print(f"\nFinal status after {int((time.time()-start_time)//1)} seconds:")
print(f"Status (private): {run._plugin_run._status}")
print(f"Has Triggered:    {run.has_triggered}")
print(f"Is Running:       {run.is_running}")
print(f"Successful:       {run.successful}")
print(f"Failed Steps:     {run.failed_steps}")
print(f"Exceptions:       {run.exceptions}\n")
