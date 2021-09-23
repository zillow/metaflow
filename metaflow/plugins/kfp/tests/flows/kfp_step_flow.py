from metaflow import FlowSpec, step, kfp_step, resources
from metaflow.plugins.kfp.kfp_constants import BASE_IMAGE
import os

from kubernetes import client, config

kfp_step_image = "analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py39-cpu:0.1.1402"

class KfpStepFlow(FlowSpec):
    """
    Test kfp_step(image=...)
    """

    @kfp_step(image=kfp_step_image)
    @step
    def start(self):
        print("Start step, testing for correct image.")

        # ensures this passed in image isn't the default image,
        # if it were, we wouldn't be testing the decorator
        assert BASE_IMAGE != kfp_step_image

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
        
        self.next(self.end)

    @step
    def end(self):
        print("End step.")


if __name__ == "__main__":
    KfpStepFlow()