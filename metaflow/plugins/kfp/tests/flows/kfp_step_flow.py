from metaflow import FlowSpec, step, kfp_step
import os

from kubernetes import client, config

kfp_step_image_1 = "analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py39-cpu:0.1.1402"


class KfpStepFlow(FlowSpec):
    """
    Test kfp_step(image=...)
    """

    @kfp_step(image="analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py39-cpu:0.1.1402")
    @step
    def start(self):
        print("Start step, testing for correct image.")
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
        assert_step_image("analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py39-cpu:0.1.1402")
        
        self.next(self.end)

    @kfp_step(image="analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py39-cpu:0.1.1408")
    @step
    def end(self):
        print("End step.")


if __name__ == "__main__":
    KfpStepFlow()
