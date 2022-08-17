from metaflow.plugins.argo import trigger_live_run  # trigger_live_run
import time

flow_name = "FailureFlow"  # "HelloArgoFlowTwo", "FailureFlow", "MissingFlow"
run = trigger_live_run(
    plugin_name='Argo',
    flow_name=flow_name,
    parameters=None,
    wait=False,
)

print(f"\nRunning flow {flow_name}")
start_time = time.time()
while run.is_running:
    print(f"""
In progress status after {int((time.time()-start_time)//1)} seconds:
    Status (private): {run._plugin_run._status}
    Has Triggered:    {run.has_triggered}
    Is Running:       {run.is_running}
    Successful:       {run.successful}
    Failed Steps:     {run.failed_steps}
    Exceptions:       {run.exceptions}\n""")
    time.sleep(15)

print(f"""
Completed status after {int((time.time()-start_time)//1)} seconds:
    Status (private): {run._plugin_run._status}
    Has Triggered:    {run.has_triggered}
    Is Running:       {run.is_running}
    Successful:       {run.successful}
    Failed Steps:     {run.failed_steps}
    Exceptions:       {run.exceptions}
""")

