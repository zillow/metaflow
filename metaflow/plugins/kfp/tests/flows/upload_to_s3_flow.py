from metaflow import FlowSpec, step, resources, s3_sensor, Parameter

import boto3
import time
from subprocess import run, PIPE

from os import environ
from os.path import join

from urllib.parse import urlparse

from kubernetes import config
from kubernetes.client import api_client
from kubernetes.dynamic import DynamicClient
from kubernetes.dynamic.resource import Resource, ResourceInstance

"""
This test flow uploads a file at a particular S3 location. The test flows s3_sensor_flow.py
and s3_sensor_flow_with_formatter.py then wait for this file to appear in S3 through the use
of the @s3_sensor, after which the flows proceed.
"""


def upload_file_to_s3(file_name: str) -> None:
    run(f"touch {file_name}", universal_newlines=True, stdout=PIPE, shell=True)
    # using environ with METAFLOW_DATASTORE_SYSROOT_S3 env var
    # since it is available at run time in the pods on Kubeflow
    root = urlparse(environ["METAFLOW_DATASTORE_SYSROOT_S3"])
    bucket, key = root.netloc, root.path.lstrip("/")

    s3 = boto3.resource("s3")
    s3.meta.client.upload_file(f"./{file_name}", bucket, join(key, file_name))

def delete_s3_sensor_pod_to_test_retry(workflow_name: str):
    print("workflow_name: ", workflow_name)

    namespace: str = environ.get("POD_NAMESPACE", default=None)

    dynamic_client: Resource = DynamicClient(
        api_client.ApiClient(configuration=config.load_incluster_config())
    )
    workflow_api: ResourceInstance = dynamic_client.resources.get(
        api_version="argoproj.io/v1alpha1", kind="Workflow"
    )
    workflow: ResourceInstance = workflow_api.get(
        name=workflow_name,
        namespace=namespace,
    )
    for node in workflow["status"]["nodes"]:
        node_name: str = node[0]
        node_info: dict = node[1]
        if node_info["type"] == "Pod" and "s3sensor" in node_name:
            s3_sensor_pod_name = node_name
            break
    else:
        raise ValueError("s3_sensor pod not found.")
    pod_api: ResourceInstance = dynamic_client.resources.get(
        api_version="v1", kind="Pod"
    )
    pod: ResourceInstance = pod_api.delete(
        name=s3_sensor_pod_name,
        namespace=namespace,
    )

class UploadToS3Flow(FlowSpec):
    file_name = Parameter(
        "file_name",
    )
    file_name_for_formatter_test = Parameter("file_name_for_formatter_test")
    workflow_name = Parameter(
        "workflow_name",
    )

    @step
    def start(self):     
        print("Waiting to delete pod to test s3_sensor retry...")
        time.sleep(15)
        delete_s3_sensor_pod_to_test_retry(self.workflow_name)

        print("Waiting to upload file...")
        time.sleep(50)
        print(f"Uploading {self.file_name} to S3...")
        upload_file_to_s3(self.file_name)

        print("Waiting to upload file for formatter test...")
        time.sleep(100)
        print(f"Uploading {self.file_name_for_formatter_test} to S3...")
        upload_file_to_s3(self.file_name_for_formatter_test)

        self.next(self.end)

    @step
    def end(self):
        print("S3SensorFlow is all done.")


if __name__ == "__main__":
    UploadToS3Flow()
