from metaflow.client.trigger_live_run import trigger_live_run
import time


"""
Tests:
    - a flow that triggers a flow (outer flow)
    - plugin_name: None, KFP
    - flow_name: 
        - "LiveRunParamFlow", 
        - "TriggerSuccessFlow"
        - "TriggerFailureFlow", 
        - "TriggerMissingFlow", 
        - "TriggerOuterFlow",
        - None, 
    - template_name: "helloargoflowtwo", "failureflow", "missingflow", "hellojsontype", None
    - parameters: None, parameters_valid, parameters_invalid, parameters_alt
    
"""
# TODO: Rename all flows, tests, and files so they make sense and are consistent


def start_message(name):
    print(f"Starting test '{name}'")


def finished_message(name):
    print(f"Finished test '{name}'")


# test parameter passing

start_time = time.time()
test_name = "parameter passing"
start_message(test_name)

run = trigger_live_run(
    plugin_name='Argo',
    flow_name='LiveRunParamFlow',
    template_name=None,
    parameters={
        'str_to_list': '[100, 200, 300]',
        'str_to_dict': '{"a": 5, "b": 10}',
        'list_param': [33, 44, "r"],
        'dict_param': {"y": 5, "z": 10},
        'str_param': "NEW string",
        'date_key': "1988-10-31",
        'int_param': 999,
        'bool_param': False,
        'float_param': 789,
        'unused_param': 'not used'
    },
    wait=True,
)

finished_message(test_name)




