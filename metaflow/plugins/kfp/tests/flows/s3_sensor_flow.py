from metaflow import FlowSpec, step, resources, s3_sensor, Parameter


def formatter(path: str, flow_parameters: dict) -> str:
    return path.format(
        env=flow_parameters["env"],
        file_name=flow_parameters["file_name"]
    )


"""
This test flow ensures that @s3_sensor properly waits for path to be written
to in S3. In run_integration_tests.py, we have a special test just for this flow.
The test creates a random file and uploads it to S3, and this flow waits on the creation
of that file.
"""
@s3_sensor(
    path="s3://serve-datalake-zillowgroup/zillow/workflow_sdk/metaflow_28d/{env}/aip-integration-testing/{file_name}",
    timeout_seconds=300,
    polling_interval_seconds=5,
    path_formatter=formatter,
)
class S3SensorFlow(FlowSpec):

    s3_file = Parameter(
        "file_name",
        default="file_name"
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
