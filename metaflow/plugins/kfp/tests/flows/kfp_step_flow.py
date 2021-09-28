from metaflow import FlowSpec, step, kfp_step, environment, Parameter
import os

from kubernetes import client, config

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


class KfpStepFlow(FlowSpec):
    """
    Test kfp_step(image=...)
    """
    @step
    def start(self):
        is_on_kubernetes = os.getenv("K8S_CLUSTER_NAME")
        if is_on_kubernetes:
            # fail early: if the environment variable not present,
            # do not wait for Kubelet to not find image
            assert os.getenv("KFP_STEP_IMAGE") is not None
        self.next(self.image_test_step)

    # We default image to None in the case of local Metaflow execution,
    # since the KFP_STEP_IMAGE env var is not present on the local machine
    @kfp_step(image=os.getenv("KFP_STEP_IMAGE"))
    @step
    def image_test_step(self):
        assert_step_image(os.getenv("KFP_STEP_IMAGE"))
        self.next(self.end)

    @step
    def end(self):
        print("End step.")

if __name__ == "__main__":
    KfpStepFlow()
