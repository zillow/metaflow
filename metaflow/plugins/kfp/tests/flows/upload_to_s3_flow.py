import time
from os import environ
from os.path import join
from subprocess import PIPE, run
from urllib.parse import urlparse

import boto3
from metaflow import FlowSpec, Parameter, resources, s3_sensor, step


class UploadToS3Flow(FlowSpec):

    file_name = Parameter(
        "file_name",
    )

    @step
    def start(self):
        print("Waiting to upload file...")
        time.sleep(100)
        print(f"Uploading {self.file_name} to S3...")

        run(f"touch {self.file_name}", universal_newlines=True, stdout=PIPE, shell=True)
        # using environ with METAFLOW_DATASTORE_SYSROOT_S3 env var
        # since it is available at run time in the pods on Kubeflow
        root = urlparse(environ["METAFLOW_DATASTORE_SYSROOT_S3"])
        bucket, key = root.netloc, root.path.lstrip("/")

        s3 = boto3.resource("s3")
        s3.meta.client.upload_file(
            f"./{self.file_name}", bucket, join(key, self.file_name)
        )
        self.next(self.end)

    @step
    def end(self):
        print("S3SensorFlow is all done.")


if __name__ == "__main__":
    UploadToS3Flow()
