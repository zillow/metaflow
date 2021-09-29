from typing import NamedTuple
import os

from aip_kfp_sdk.components.component import kfp_component

from metaflow import FlowSpec, step, kfp, resources
from kubernetes import client, config


@kfp_component(use_code_pickling=False)
def div_mod(
    dividend: int, divisor: int
) -> NamedTuple("result", [("quotient", int), ("remainder", int)]):
    print(f"dividend={dividend}, divisor={divisor}")
    return divmod(dividend, divisor)


def assert_step_image(kfp_step_image: str):
    is_on_kubernetes = os.getenv("K8S_CLUSTER_NAME")
    if is_on_kubernetes:  # only perform this test on the cluster, not on local machine
        config.load_incluster_config()
        core_api_instance = client.CoreV1Api()

        current_pod_name = os.environ.get("HOSTNAME", None)
        current_pod_namespace = os.environ.get("POD_NAMESPACE", None)

        if current_pod_name and current_pod_namespace:
            pod_detail = core_api_instance.read_namespaced_pod(
                namespace=current_pod_namespace, name=current_pod_name
            )

            for container_status in pod_detail.status.container_statuses:
                if container_status.name == "main":
                    assert container_status.image == kfp_step_image


class KfpFlow(FlowSpec):
    """
    Test adding a KFP Component and decorators and testing use of image=...
    """

    @resources(cpu=5, memory="1G")
    @step
    def start(self):
        """
        All flows must have a step named 'start' that is the first step in the flow.
        """
        self.dividend = 26
        self.divisor = 7
        self.next(self.end)
        env_image_tag = os.getenv("IMAGE_TAG")
        assert not env_image_tag.endswith("_kfp_step")
        assert_step_image(env_image_tag)

    @kfp(
        preceding_component=div_mod,
        preceding_component_inputs="dividend divisor",
        preceding_component_outputs="quotient remainder",
        image=os.getenv("KFP_STEP_IMAGE"),
    )
    @step
    def end(self):
        """
        Validate that the results of the preceding div_mod KFP step are bound
        to Metaflow state. We use os.getenv instead of environ to ensure this flow
        runs correctly on the local machine (which doesn't have the KFP_STEP_IMAGE
        and IMAGE_TAG env vars).
        """
        print(f"quotient={type(self.quotient)}, remainder={type(self.remainder)}")
        print(f"quotient={self.quotient}, remainder={self.remainder}")
        assert int(self.quotient) == 3
        assert int(self.remainder) == 5
        env_image_tag = os.getenv("IMAGE_TAG")
        assert env_image_tag.endswith("_kfp_step")
        assert_step_image(env_image_tag)


if __name__ == "__main__":
    KfpFlow()
