from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

from types import FunctionType
from typing import Tuple


def identity_formatter(path: str, flow_parameters_json: dict) -> str:
    return path


"""
@s3_sensor is implemented as a flow decorator that's used by customers to ensure 
a workflow only begins after a key in S3 has been written to. 

Example usage:

    def formatter(path: str, flow_parameters_json: dict) -> str:
        return path.replace("FLOW_ID", flow_parameters_json["FLOW_ID"])

    @s3_sensor(
        path="s3://aip-example-sandbox/metaflow/S3SensorFlow/data/$date/61/FLOW_ID",
        timeout_seconds=3600, # 1 hour
        polling_interval_seconds=90,
        formatter=formatter
    )
    class S3SensorFlow(FlowSpec):    
        @step
        def start(self):
            print("S3SensorFlow is starting.")
            self.next(self.hello)

        @step
        def hello(self):
            self.next(self.end)

        @step
        def end(self):
            print("S3SensorFlow is all done.")
"""


class S3SensorDecorator(FlowDecorator):
    name = "s3_sensor"
    defaults = {
        "path": "",
        "timeout_seconds": 3600,
        "polling_interval_seconds": 300,
        "formatter": identity_formatter,
    }

    def flow_init(self, flow, graph, environment, datastore, logger, echo, options):
        self.path = self.attributes["path"]
        self.timeout_seconds = self.attributes["timeout_seconds"]
        self.polling_interval_seconds = self.attributes["polling_interval_seconds"]
        self.formatter = self.attributes["formatter"]

        if not self.path:
            raise MetaflowException("You must specify a S3 path within @s3_sensor.")

        if not self.path.startswith("s3://"):
            raise MetaflowException("Your path must be prefixed with s3://")

        prefix_removed_path = self.path.replace("s3://", "", 1)
        if len(prefix_removed_path.split("/", 1)) == 1:
            raise MetaflowException(
                "Your path must contain both a bucket and key, separated by a slash."
            )

        if not isinstance(self.formatter, FunctionType):
            raise MetaflowException("formatter must be a function.")
