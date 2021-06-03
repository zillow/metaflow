import os
import pprint
import subprocess
from typing import Dict, List

from kubernetes.client import (
    V1EnvVar,
    V1EnvVarSource,
    V1ObjectFieldSelector,
    V1ResourceFieldSelector,
    V1NodeSelectorTerm,
    V1Toleration,
)

from kubernetes import client, config

from metaflow import FlowSpec, step, environment, resources, current, accelerator
from metaflow.exception import MetaflowException


def get_env_vars(env_resources: Dict[str, str]) -> List[V1EnvVar]:
    res = []
    for name, resource in env_resources.items():
        res.append(
            V1EnvVar(
                # this is used by some functions of operator-sdk
                # it uses this environment variable to get the pods
                name=name,
                value_from=V1EnvVarSource(
                    resource_field_ref=V1ResourceFieldSelector(
                        container_name="main",
                        resource=resource,
                        divisor="1m" if "cpu" in resource else "1",
                    )
                ),
            )
        )
    return res


kubernetes_vars = get_env_vars(
    {
        "LOCAL_STORAGE": "requests.ephemeral-storage",
        "LOCAL_STORAGE_LIMIT": "limits.ephemeral-storage",
        "CPU": "requests.cpu",
        "CPU_LIMIT": "limits.cpu",
        "MEMORY": "requests.memory",
        "MEMORY_LIMIT": "limits.memory",
    }
)
kubernetes_vars.append(
    V1EnvVar(
        name="POD_NAME",
        value_from=V1EnvVarSource(
            field_ref=V1ObjectFieldSelector(field_path="metadata.name")
        ),
    )
)

annotations = {
    "metaflow.org/flow_name": "MF_NAME",
    "metaflow.org/step": "MF_STEP",
    "metaflow.org/run_id": "MF_RUN_ID",
    "metaflow.org/experiment": "MF_EXPERIMENT",
    "metaflow.org/tag_metaflow_test": "MF_TAG_METAFLOW_TEST",
    "metaflow.org/tag_test_t1": "MF_TAG_TEST_T1",
}
for annotation, env_name in annotations.items():
    kubernetes_vars.append(
        V1EnvVar(
            name=env_name,
            value_from=V1EnvVarSource(
                field_ref=V1ObjectFieldSelector(
                    field_path=f"metadata.labels['{annotation}']"
                )
            ),
        )
    )


class ResourcesFlow(FlowSpec):
    @accelerator
    @resources(
        local_storage="100",
        local_storage_limit="242",
        cpu="0.1",
        cpu_limit="0.6",
        memory="1G",
        memory_limit="2G",
    )
    @environment(  # pylint: disable=E1102
        vars={"MY_ENV": "value"}, kubernetes_vars=kubernetes_vars
    )
    @step
    def start(self):
        # pprint.pprint(dict(os.environ))
        print("=====")

        # test simple environment var
        assert os.environ.get("MY_ENV") == "value"

        # test kubernetes_vars
        assert "resourcesflow" in os.environ.get("POD_NAME")
        assert os.environ.get("CPU") == "100"
        assert os.environ.get("CPU_LIMIT") == "600"
        assert os.environ.get("LOCAL_STORAGE") == "100000000"
        assert os.environ.get("LOCAL_STORAGE_LIMIT") == "242000000"
        assert os.environ.get("MEMORY") == "1000000000"
        assert os.environ.get("MEMORY_LIMIT") == "2000000000"

        assert os.environ.get("MF_NAME") == current.flow_name
        assert os.environ.get("MF_STEP") == current.step_name
        assert os.environ.get("MF_RUN_ID") == current.run_id
        assert os.environ.get("MF_EXPERIMENT") == "metaflow_test"
        assert os.environ.get("MF_TAG_METAFLOW_TEST") == "true"
        assert os.environ.get("MF_TAG_TEST_T1") == "true"

        # test accelerator usage
        # specifically, ensure correct node selectors and tolerations are applied
        config.load_incluster_config()
        core_api_instance = client.CoreV1Api()

        skip_container_names = {"wait", "istio-proxy", "istio-init", "queue-proxy"}
        current_pod_name = os.environ.get("HOSTNAME", None)
        current_pod_namespace = os.environ.get("POD_NAMESPACE", None)

        def validate_node_selector_term(node_selector_term: V1NodeSelectorTerm) -> bool:
            for affinity_match_expression in node_selector_term.match_expressions:
                if (
                    affinity_match_expression.key == "k8s.amazonaws.com/accelerator"
                    and affinity_match_expression.operator == "In"
                    and "nvidia-tesla-v100" in affinity_match_expression.values
                ):
                    return True
            return False

        def validate_toleration(toleration: V1Toleration) -> bool:
            if (
                toleration.effect == "NoSchedule"
                and toleration.key == "k8s.amazonaws.com/accelerator"
                and toleration.operator == "Equal"
                and toleration.value == "nvidia-tesla-v100"
            ):
                return True
            return False

        if current_pod_name and current_pod_namespace:
            pod_detail = core_api_instance.read_namespaced_pod(
                namespace=current_pod_namespace, name=current_pod_name
            )

            for (
                node_selector_term
            ) in (
                pod_detail.spec.affinity.node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms
            ):
                if validate_node_selector_term(node_selector_term):
                    break
            else:
                raise MetaflowException(
                    "Correct node affinity for P3 instance type not found."
                )

            for toleration in pod_detail.spec.tolerations:
                if validate_toleration(toleration):
                    break
            else:
                raise MetaflowException(
                    "Correct pod toleration for P3 instance type not found."
                )
        else:
            raise MetaflowException("Could not verify accelerator usage.")

        self.items = [1, 2]
        self.next(self.split_step, foreach="items")

    @resources(volume="11G")
    @step
    def split_step(self):
        output = subprocess.check_output(
            "df -h | grep /opt/metaflow_volume", shell=True
        )
        assert "11G" in str(output)
        self.next(self.join_step)

    @resources(volume="12G")
    @step
    def join_step(self, inputs):
        output = subprocess.check_output(
            "df -h | grep /opt/metaflow_volume", shell=True
        )
        assert "12G" in str(output)

        self.next(self.end)

    @step
    def end(self):
        print("All done.")


if __name__ == "__main__":
    ResourcesFlow()
