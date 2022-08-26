from metaflow.client.trigger_live_run import trigger_live_run
from metaflow.exception import MetaflowException
import time
from unittest import TestCase


"""
Tests Outline:
    - Parameter Passing (flow name "LiveRunParamFlow")
        - successful parameter passing
        - wrong type parameter passing (eg, type=set)
    - Flow Identification (flow name "LiveRunSimpleFlow")
        - id by flow name
        - id by template name
        - id by both (matching)
        - id by both (mismatch)
        - id by neither flow name nor template name 
    - Plugin Name (all other tests correctly use 'Argo')
        - None
        - 'FakePlugin'
    - Other Tests
        - timeout error (flow takes too long - set wait_timeout to 0)
        - failure flow (flow name "LiveRunFailureFlow")
        - missing flow (flow name "LiveRunMissingFlow")
        - flow that triggers a flow (flow name "LiveRunFlowTriggeringFlow")
    - Features/aspects tested but w/o their own named tests
        - Wait Feature - no separate tests because both states (True & False)
            are incorporated into Flow Identification section
"""


def start_message(name):
    print(f"\nStarting test '{name}'")


def finished_message(name):
    print(f"Finished test '{name}'")


test_case = TestCase()
"""
"""

### Parameter Passing (flow name "LiveRunParamFlow") ###

# successful parameter passing test
test_name = "parameter passing"
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
assert run.successful is True
finished_message(test_name)

# wrong type parameter passing test (with set as parameter)
test_name = "wrong type parameter passing"
start_message(test_name)
with test_case.assertRaises(TypeError):
    run = trigger_live_run(
        plugin_name="Argo",
        flow_name="LiveRunParamFlow",
        template_name=None,
        parameters={"set": {"a", "z"}},
        wait=True,
    )
finished_message(test_name)

### Flow Identification (flow name "LiveRunSimpleFlow") ###

# id by flow name
test_name = "id by flow name"
start_message(test_name)
run = trigger_live_run(
    plugin_name="Argo",
    flow_name="LiveRunSimpleFlow",
    template_name=None,
    parameters=None,
    wait=True,
)
assert run.flow_name == "LiveRunSimpleFlow"
assert "argo-liverunsimpleflow-" in run.run_id
assert run.has_triggered is True
assert run.is_running is False
assert run.successful is True
assert not run.failed_steps
assert not run.exceptions
finished_message(test_name)

# id by template name
test_name = "id by template name"
start_message(test_name)
run = trigger_live_run(
    plugin_name="Argo",
    flow_name=None,
    template_name="liverunsimpleflow",
    parameters=None,
    wait=False,
)
assert run.flow_name == "LiveRunSimpleFlow"
assert "argo-liverunsimpleflow-" in run.run_id
assert run.has_triggered is True
assert run.is_running is True
assert run.successful is False
assert not run.failed_steps
assert not run.exceptions
finished_message(test_name)

# id by both (matching)
test_name = "id by both (matching)"
start_message(test_name)
run = trigger_live_run(
    plugin_name="Argo",
    flow_name="LiveRunSimpleFlow",
    template_name="liverunsimpleflow",
    parameters=None,
    wait=False,
)
assert run.has_triggered is True
assert run.is_running is True
assert run.successful is False
finished_message(test_name)

# id by both (mismatch)
test_name = "id by both (mismatch)"
start_message(test_name)
with test_case.assertRaises(ValueError):
    run = trigger_live_run(
        plugin_name="Argo",
        flow_name="LiveRunParamFlow",
        template_name="liverunsimpleflow",
        parameters=None,
        wait=False,
    )
finished_message(test_name)

# id by neither flow name nor template name
test_name = "id by neither flow name nor template name"
start_message(test_name)
with test_case.assertRaises(ValueError):
    run = trigger_live_run(
        plugin_name="Argo",
        flow_name=None,
        template_name=None,
        parameters=None,
        wait=False,
    )
finished_message(test_name)

### Plugin Name (all other tests correctly use 'Argo') ###

# plugin name is None
test_name = "plugin name is None"
start_message(test_name)
with test_case.assertRaises(ValueError):
    run = trigger_live_run(
        plugin_name=None,
        flow_name="LiveRunSimpleFlow",
        template_name=None,
        parameters=None,
        wait=False,
    )
finished_message(test_name)

# plugin name is 'FakePlugin'
test_name = "plugin name is 'FakePlugin'"
start_message(test_name)
with test_case.assertRaises(ValueError):
    run = trigger_live_run(
        plugin_name='FakePlugin',
        flow_name="LiveRunSimpleFlow",
        template_name=None,
        parameters=None,
        wait=False,
    )
finished_message(test_name)

### Other Tests ###

# timeout error (flow takes too long - set wait_timeout to 0)
test_name = "timeout error"
start_message(test_name)
with test_case.assertRaises(TimeoutError):
    run = trigger_live_run(
        plugin_name="Argo",
        flow_name="LiveRunSimpleFlow",
        template_name=None,
        parameters=None,
        wait=True,
        wait_timeout=0,
    )
finished_message(test_name)

# failure flow (flow name "LiveRunFailureFlow")
test_name = "failure flow"
start_time = time.time()
start_message(test_name)
run = trigger_live_run(
    plugin_name="Argo",
    flow_name="LiveRunFailureFlow",
    template_name=None,
    parameters=None,
    wait=False,
)
assert run.has_triggered is True
assert run.is_running is True
assert run.successful is False

while run.is_running:
    time.sleep(5)

assert run.has_triggered is True
assert run.is_running is False
assert run.successful is False
assert run.failed_steps == ['failure_step']
for key in run.exceptions:
    assert 'failure_step' in key
finished_message(test_name)

# missing flow (flow name "LiveRunMissingFlow")
test_name = "missing flow"
start_message(test_name)
with test_case.assertRaises(MetaflowException):
    run = trigger_live_run(
        plugin_name="Argo",
        flow_name="LiveRunMissingFlow",
        template_name=None,
        parameters=None,
        wait=True,
    )
finished_message(test_name)
"""
# flow that triggers a flow (flow name "LiveRunFlowTriggeringFlow")
test_name = "flow that triggers a flow"
start_message(test_name)
run = trigger_live_run(
    plugin_name="Argo",
    flow_name="LiveRunFlowTriggeringFlow",
    wait=True,
)
assert run.successful is True
finished_message(test_name)
"""
