import tempfile
from os.path import join
from typing import Dict

import yaml
from subprocess_tee import run

from . import _python


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
        for toleration in step_template["tolerations"]
    )


def test_compile_only_accelerator_test() -> None:
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
            step_templates[step["name"]] = step

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
