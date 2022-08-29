from metaflow.client.trigger_live_run import trigger_live_run
from metaflow.exception import MetaflowException
import time
import unittest


# TODO:
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


class TestTriggerLiveRun(unittest.TestCase):

    # Parameter Passing Tests

    def test_parameter_passing_success(self):
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
        self.assertTrue(run.successful)

    def test_parameter_passing_failure(self):
        with self.assertRaises(TypeError):
            trigger_live_run(
                plugin_name="Argo",
                flow_name="LiveRunParamFlow",
                template_name=None,
                parameters={"set": {"a", "z"}},
                wait=True,
            )

    # Flow Identification Tests

    def test_id_by_flow_name(self):
        run = trigger_live_run(
            plugin_name="Argo",
            flow_name="LiveRunSimpleFlow",
            template_name=None,
            parameters=None,
            wait=True,
        )
        self.assertEqual(run.flow_name, "LiveRunSimpleFlow")
        self.assertTrue("argo-liverunsimpleflow-" in run.run_id)
        self.assertTrue(run.has_triggered)
        self.assertFalse(run.is_running)
        self.assertTrue(run.successful)
        self.assertEqual(len(run.failed_steps), 0)
        self.assertEqual(len(run.exceptions), 0)

    def test_id_by_template_name(self):
        run = trigger_live_run(
            plugin_name="Argo",
            flow_name=None,
            template_name="liverunsimpleflow",
            parameters=None,
            wait=False,
        )
        self.assertEqual(run.flow_name, "LiveRunSimpleFlow")
        self.assertTrue("argo-liverunsimpleflow-" in run.run_id)
        self.assertTrue(run.has_triggered)
        self.assertTrue(run.is_running)
        self.assertFalse(run.successful)
        self.assertEqual(len(run.failed_steps), 0)
        self.assertEqual(len(run.exceptions), 0)

    def test_id_by_matching_flow_and_template_names(self):
        run = trigger_live_run(
            plugin_name="Argo",
            flow_name="LiveRunSimpleFlow",
            template_name="liverunsimpleflow",
            parameters=None,
            wait=False,
        )
        self.assertTrue(run.has_triggered)
        self.assertTrue(run.is_running)
        self.assertFalse(run.successful)

    def test_id_by_mismatching_flow_and_template_names(self):
        with self.assertRaises(ValueError):
            trigger_live_run(
                plugin_name="Argo",
                flow_name="LiveRunParamFlow",
                template_name="liverunsimpleflow",
                parameters=None,
                wait=False,
            )

    def test_id_with_no_flow_or_template_name(self):
        with self.assertRaises(ValueError):
            trigger_live_run(
                plugin_name="Argo",
                flow_name=None,
                template_name=None,
                parameters=None,
                wait=False,
            )

    # Plugin Name Tests

    def test_plugin_name_is_none(self):
        with self.assertRaises(ValueError):
            trigger_live_run(
                plugin_name=None,
                flow_name="LiveRunSimpleFlow",
                template_name=None,
                parameters=None,
                wait=False,
            )

    def test_plugin_name_not_supported(self):
        with self.assertRaises(ValueError):
            trigger_live_run(
                plugin_name="FakePlugin",
                flow_name="LiveRunSimpleFlow",
                template_name=None,
                parameters=None,
                wait=False,
            )

    # Other Tests

    def test_timeout_error(self):
        with self.assertRaises(TimeoutError):
            trigger_live_run(
                plugin_name="Argo",
                flow_name="LiveRunSimpleFlow",
                template_name=None,
                parameters=None,
                wait=True,
                wait_timeout=0,
            )

    def test_failure_flow(self):
        # trigger run
        run = trigger_live_run(
            plugin_name="Argo",
            flow_name="LiveRunFailureFlow",
            template_name=None,
            parameters=None,
            wait=False,
        )
        self.assertTrue(run.has_triggered)
        self.assertTrue(run.is_running)
        self.assertFalse(run.successful)

        # wait to complete and test final statuses
        while run.is_running:
            time.sleep(5)
        self.assertTrue(run.has_triggered)
        self.assertFalse(run.is_running)
        self.assertFalse(run.successful)
        self.assertEqual(run.failed_steps, ["failure_step"])
        for key in run.exceptions:
            self.assertTrue("failure_step" in key)

    def test_missing_flow(self):
        with self.assertRaises(MetaflowException):
            run = trigger_live_run(
                plugin_name="Argo",
                flow_name="LiveRunMissingFlow",
                template_name=None,
                parameters=None,
                wait=True,
            )

    def test_flow_triggering_flow(self):
        # TODO: LiveRunFlowTriggeringFlow uses workaround to download k8s.
        # TODO (continued): k8s should be dependency in the image used for testing.
        run = trigger_live_run(
            plugin_name="Argo",
            flow_name="LiveRunFlowTriggeringFlow",
            wait=True,
        )
        self.assertTrue(run.successful)


if __name__ == "__main__":
    unittest.main()
