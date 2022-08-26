from metaflow.client.trigger_live_run import trigger_live_run
import time


"""
Tests:
    - [DONE] parameter passing (flow name "LiveRunParamFlow")
    - simple flow (flow name "LiveRunSimpleFlow")
        - by flow name
        - by template name
        - by both (matching)
        - by both (mismatch)
        - by neither flow name or template name
    - failure flow (flow name "LiveRunFailureFlow")
    - missing flow (flow name "LiveRunMissingFlow")
    - a flow that triggers a flow (flow name "LiveRunOuterFlow")
    - plugin_name inputs: 
        - None
        - 'KFP'
    - wait feature
    - TEST ALL ERRORS in feature
"""
# TODO: Rename all flows, tests, and files so they make sense and are consistent


def start_message(name):
    print(f"Starting test '{name}'")


def finished_message(name):
    print(f"Finished test '{name}'")


# parameter passing
test_name = "parameter passing"
start_time = time.time()
start_message(test_name)
run = trigger_live_run(
    plugin_name="Argo",
    flow_name="LiveRunParamFlow",
    template_name=None,
    parameters={
        "str_to_list": "[100, 200, 300]",
        "str_to_dict": '{"a": 5, "b": 10}',
        "list_param": [33, 44, "r"],
        "dict_param": {"y": 5, "z": 10},
        "str_param": "NEW string",
        "date_key": "1988-10-31",
        "int_param": 999,
        "bool_param": False,
        "float_param": 789.654,
        "unused_param": "not used",
    },
    wait=True,
)
finished_message(test_name)
