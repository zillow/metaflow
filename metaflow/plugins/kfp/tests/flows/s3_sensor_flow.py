from metaflow import FlowSpec, step, resources, s3_sensor, Parameter


def formatter(path: str, flow_parameters_json: dict) -> str:
    return path.replace("CODE_PACKAGE_ID", flow_parameters_json["code_package_id"])


@s3_sensor(
    path="s3://serve-datalake-zillowgroup/zillow/workflow_sdk/metaflow_28d/dev/aip-integration-testing/HelloFlow/data/6f/CODE_PACKAGE_ID",
    timeout_seconds=45,
    polling_interval_seconds=1,
    formatter=formatter,
)
class S3SensorFlow(FlowSpec):

    # This parameter is just for testing @s3_sensor. In an actual flow,
    # it doesn't make sense to pass the code package's metadata
    # as a parameter, as that is transparently done behind the scenes.
    code_package_id = Parameter(
        "code_package_id",
        default="6ff80f2870922f04ed6960dba2f23c7223e699a7",
    )

    @step
    def start(self):
        print("S3SensorFlow is starting.")
        self.next(self.hello)

    @step
    def hello(self):
        print("Hi!")
        self.next(self.end)

    @step
    def end(self):
        print("S3SensorFlow is all done.")


if __name__ == "__main__":
    S3SensorFlow()
