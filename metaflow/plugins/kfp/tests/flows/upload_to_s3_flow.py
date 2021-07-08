from metaflow import FlowSpec, step, resources, s3_sensor, Parameter

import boto3
from subprocess import run, PIPE

class UploadToS3Flow(FlowSpec):

    file_name = Parameter(
        "file_name",
    )
    env = Parameter(
        "env"
    )
    @step
    def start(self):
        print(f"Uploading {self.file_name} to S3...")
        self.next(self.end)

        run(
            f"touch {self.file_name}",
            universal_newlines=True,
            stdout=PIPE,
            shell=True
        )

        print("File name: ", self.file_name)

        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(
            f"./{self.file_name}", 
            "serve-datalake-zillowgroup",
            f"zillow/workflow_sdk/metaflow_28d/{self.env}/aip-integration-testing/{self.file_name}"
        )

    @step
    def end(self):
        print("S3SensorFlow is all done.")


if __name__ == "__main__":
    UploadToS3Flow()
