from metaflow import FlowSpec, step, resources, s3_sensor, Parameter


"""
This test flow ensures that @s3_sensor properly waits for path to be written
to in S3. In run_integration_tests.py, we have a special test just for this flow.
The test creates a random file and uploads it to S3, and this flow waits on the creation
of that file.
"""
@s3_sensor(
    path="s3://serve-datalake-zillowgroup/zillow/workflow_sdk/metaflow_28d/{env}/aip-integration-testing/{file_name}",
    #path="s3://aip-example-sandbox/metaflow/KfpFlow/data/de/dec9ee1662789e9ffbbdc5396922802d1fc177f5",
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
    #@resources(volume="1G")
    @step
    def start(self):
        print("S3SensorFlow is starting.")
        self.next(self.end)

    #@resources(volume="1G")
    @step
    def end(self):
        print("S3SensorFlow is all done.")


if __name__ == "__main__":
    S3SensorFlow()
