from metaflow.plugins.argo import trigger_live_run  # trigger_live_run
import time

flow_name = None  # "HelloJSONType", "HelloArgoFlowTwo", None
template_name = "hellojsontype"  # "hellojsontype", "helloargoflowtwo", None
parameters_off = None
parameters_on = {'str_to_list': "[1, 2, 3, 4, 5]",
                 'str_to_dict': '{"c": [1, 2, 3], "d": 99}',
                 'list_param': [100, 200, 300],
                 'dict_param': {'one': 100, 'two': 200, 'three': 300},
                 }

parameters = parameters_on  # parameters_on, parameters_off

run = trigger_live_run(
    plugin_name='Argo',
    flow_name=flow_name,
    alt_flow_id_info={'template_name': template_name},
    parameters=parameters,
    wait=False,
)

print(f"\nRunning flow {run.flow_name}")
start_time = time.time()
while run.is_running:
    print(f"Status (private): '{run._plugin_run._status}' after {int((time.time() - start_time) // 1)} seconds")
    run._plugin_run._print_status()
    time.sleep(15 - (time.time() - start_time) % 15)

print(f"Final status (private): {run._plugin_run._status} after {int((time.time() - start_time) // 1)} seconds")
run._plugin_run._print_status()
