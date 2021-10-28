from subprocess import run, PIPE

import tempfile
from os import listdir
from os.path import isfile, join

import yaml
from subprocess_tee import run
from typing import List, Dict

import pytest

from metaflow import R

"""
To run these tests from your terminal, go to the tests directory and run: 
`python -m pytest -s -n 3 run_integration_tests.py`

This script runs all the flows in the `flows` directory. It creates
each kfp run, waits for the run to fully complete, and prints whether
or not the run was successful. It also checks to make sure the logging
functionality works.

More specifically, the tests spawn KFP runs and ensure the spawning processes
have a returncode of 0. If any test fails within KFP, an exception
is raised, the test fails, and the user can access the run link to the failed
KFP run.

Parameters:
-n: specifies the number of parallel processes used by PyTest.

Sometimes, the tests may fail on KFP due to resource quota issues. If they do,
try reducing -n (number of parallel processes) so less simultaneous
KFP runs will be scheduled.

"""

non_standard_test_flows = [
    "check_error_handling_flow.py",
    "raise_error_flow.py",
    "s3_sensor_flow.py",
    "s3_sensor_with_formatter_flow.py",
    "toleration_and_affinity_flow.py",
    "upload_to_s3_flow.py",
]


def _python():
    if R.use_r():
        return "python3"
    else:
        return "python"


def obtain_flow_file_paths(flow_dir_path: str) -> List[str]:
    file_paths = [
        file_name
        for file_name in listdir(flow_dir_path)
        if isfile(join(flow_dir_path, file_name))
        and not file_name.startswith(".")
        and file_name not in non_standard_test_flows
    ]
    return file_paths


# this test ensures the integration tests fail correctly
def test_raise_failure_flow(pytestconfig) -> None:
    test_cmd = (
        f"{_python()} flows/raise_error_flow.py --datastore=s3 kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--max-parallelism 3 --experiment metaflow_test --tag test_t1 "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    run_and_wait_process = run(
        test_cmd,
        universal_newlines=True,
        stdout=PIPE,
        shell=True,
    )
    # this ensures the integration testing framework correctly catches a failing flow
    # and reports the error
    assert run_and_wait_process.returncode == 1

    return


@pytest.mark.parametrize("flow_file_path", obtain_flow_file_paths("flows"))
def test_flows(pytestconfig, flow_file_path: str) -> None:
    full_path = join("flows", flow_file_path)
    # In the process below, stdout=PIPE because we only want to capture stdout.
    # The reason is that the click echo function prints to stderr, and contains
    # the main logs (run link, graph validation, package uploading, etc). We
    # want to ensure these logs are visible to users and not captured.
    # We use the print function in kfp_cli.py to print a magic token containing the
    # run id and capture this to correctly test logging. See the
    # `check_valid_logs_process` process.

    test_cmd = (
        f"{_python()} {full_path} --datastore=s3 kfp run "
        f"--wait-for-completion --workflow-timeout 1800 "
        f"--max-parallelism 3 --experiment metaflow_test --tag test_t1 "
    )
    if pytestconfig.getoption("image"):
        test_cmd += (
            f"--no-s3-code-package --base-image {pytestconfig.getoption('image')}"
        )

    run_and_wait_process = run(
        test_cmd,
        universal_newlines=True,
        stdout=PIPE,
        shell=True,
    )
    assert run_and_wait_process.returncode == 0


def exists_nvidia_accelerator(node_selector_term: Dict) -> bool:
    for affinity_match_expression in node_selector_term["matchExpressions"]:
        if (
            affinity_match_expression["key"] == "k8s.amazonaws.com/accelerator"
            and affinity_match_expression["operator"] == "In"
            and "nvidia-tesla-v100" in affinity_match_expression["values"]
        ):
            return True
    return False


def has_node_toleration(
    step_template, key, value, operator="Equal", effect="NoSchedule"
):
    return any(
        toleration.get("key") == key
        and toleration.get("value") == value
        and toleration.get("operator") == operator
        and toleration.get("effect") == effect
        for toleration in step_template.get("tolerations", [])
    )


def test_toleration_and_affinity_compile_only() -> None:
    step_templates = {}
    with tempfile.TemporaryDirectory() as yaml_tmp_dir:
        yaml_file_path = join(yaml_tmp_dir, "toleration_and_affinity_flow.yaml")

        compile_to_yaml_cmd = (
            f"{_python()} flows/toleration_and_affinity_flow.py --datastore=s3 --with retry kfp run"
            f" --no-s3-code-package --yaml-only --pipeline-path {yaml_file_path}"
        )

        compile_to_yaml_process = run(
            compile_to_yaml_cmd,
            universal_newlines=True,
            shell=True,
        )
        assert compile_to_yaml_process.returncode == 0

        with open(f"{yaml_file_path}", "r") as stream:
            try:
                flow_yaml = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        for step in flow_yaml["spec"]["templates"]:
            # step name in yaml use "-" in place of "_"
            step_templates[step["name"].replace("-", "_")] = step

    # Test accelerator deco: Both affinity and toleration need to be added
    assert any(
        exists_nvidia_accelerator(node_selector_term)
        for node_selector_term in step_templates["start"]["affinity"]["nodeAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"
        ]["nodeSelectorTerms"]
    )
    assert has_node_toleration(
        step_template=step_templates["start"],
        key="k8s.amazonaws.com/accelerator",
        value="nvidia-tesla-v100",
    )

    # Test toleration generated from resource spec for CPU pods
    assert not has_node_toleration(
        step_template=step_templates["small_default_pod"],
        key="node.kubernetes.io/instance-type",
        value="r5.12xlarge",
    )
    assert not has_node_toleration(
        step_template=step_templates["small_cpu_pod"],
        key="node.kubernetes.io/instance-type",
        value="r5.12xlarge",
    )
    assert not has_node_toleration(
        step_template=step_templates["small_memory_pod"],
        key="node.kubernetes.io/instance-type",
        value="r5.12xlarge",
    )
    assert has_node_toleration(
        step_template=step_templates["large_cpu_pod"],
        key="node.kubernetes.io/instance-type",
        value="r5.12xlarge",
    )
    assert has_node_toleration(
        step_template=step_templates["large_memory_pod"],
        key="node.kubernetes.io/instance-type",
        value="r5.12xlarge",
    )
    assert has_node_toleration(
        step_template=step_templates["large_memory_cpu_pod"],
        key="node.kubernetes.io/instance-type",
        value="r5.12xlarge",
    )
