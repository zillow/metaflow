from metaflow.client.trigger_live_run import trigger_live_run
import time


"""
Tests:
    - a flow that triggers a flow (outer flow)
    - plugin_name: None, KFP
    - flow_name: "HelloArgoFlowTwo", "TriggerFailureFlow", "MissingFlow", "TriggerSuccessFlow", None, OuterFlow
    - template_name: "helloargoflowtwo", "failureflow", "missingflow", "hellojsontype", None
    - parameters: None, parameters_valid, parameters_invalid, parameters_alt
    
"""
# TODO: Rename all flows, tests, and files so they make sense and are consistent


test_run_conditions = {}


# normal run test


def normal_run_while_running_test(live_run):
    print("doing while-running test")


def normal_run_post_run_test(live_run):
    print("doing post-run test")


test_run_conditions["normal_run"] = {
    "plugin_name": "Argo",
    "flow_name": "HelloArgoFlowTwo",
    "template_name": None,
    "parameters": None,
    "wait": False,
    "while_running_test": normal_run_while_running_test,
    "post_run_test": normal_run_post_run_test,
}

for test_name, conditions in test_run_conditions.items():
    start_time = time.time()
    print(f"Running test: '{test_name}'")

    run = trigger_live_run(
        plugin_name=conditions.get("plugin_name"),
        flow_name=conditions.get("flow_name"),
        template_name=conditions.get("template_name"),
        parameters=conditions.get("parameters"),
        wait=conditions.get("wait", False),
    )

    # Where do I put assert statements? And how do I personalize them for each test?

    while_running_test = conditions.get("while_running_test")
    post_run_test = conditions.get("post_run_test")
    # TODO: do we want to run this test every loop? first time only? every minute? when status changes?
    #
    old_status = None
    while run.is_running:
        if run._status != old_status:
            old_status = run._status
            while_running_test(run)
        time.sleep(
            1
        )  # make at least one flow sleep for 5 seconds so that running phase is captured

    post_run_test(run)
    print(f"done w/ testing '{test_name}'\n")


# paramaters to test
parameters_valid = {
    "str_to_list": "[1, 2, 3, 4, 5]",
    "str_to_dict": '{"c": [1, 2, 3], "d": 99, "is_input_param": "yes"}',
    "list_param": ["this", "is", "a", "list"],
    "dict_param": {"e": [4, 5, 6], "f": 22, "starts_as_dict": "yes"},
    "reg_str": "this is a string",
    "date_key": "2020-01-01",
    "integer": 1,
    "boolean": True,
    "float": 123.456,
    # TODO: Add all params for testing into TriggerSuccessFlow flow
}
# NOTE: Metaflow Parameter objects only support complex data types in JSON formats.
# None, parameters_valid, parameters_invalid, parameters_alt
parameters_invalid = {
    "set_param": {1, 2, 3, 4, 5},
    "set_in_list_param": {"c": [1, 2, 3], "d": 99, "set_here": {"a"}},
}
parameters_alt = {"alt_param": [1, 2, 999]}
