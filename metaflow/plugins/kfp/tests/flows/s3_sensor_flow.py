from metaflow import FlowSpec, step, resources, s3_sensor, Parameter

from os import getenv
from os.path import join
"""
This test flow ensures that @s3_sensor properly waits for path to be written
to in S3. In run_integration_tests.py, we have a special test just for this flow.
The test creates a random file and uploads it to S3, and this flow waits on the creation
of that file.
"""
@s3_sensor(
    path=join(getenv("METAFLOW_DATASTORE_SYSROOT_S3", "s3://random_bucket/random_key"), "{file_name}"),
    timeout_seconds=600,
    polling_interval_seconds=5,
)
class S3SensorFlow(FlowSpec):

    file_name = Parameter(
        "file_name",
    )
    env = Parameter(
        "env"
    )
    @step
    def start(self):
        print("S3SensorFlow is starting.")
        self.next(self.end)

    @step
    def end(self):
        print("S3SensorFlow is all done.")


if __name__ == "__main__":
    S3SensorFlow()
