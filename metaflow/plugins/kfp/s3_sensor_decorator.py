from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

from types import FunctionType
from typing import Tuple
from urllib.parse import urlparse

"""
Within identity_formatter, which is passed in as the path_formatter parameter,
customers have access to all variables in flow_parameters_json (which 
include all parameters passed in through the flow at compile and run time by
KFP) as well as a number of environment variables.

Please see metaflow/plugin/environment_decorator.py for details on how
to add environment variables to be accessible within steps.
"""


def identity_formatter(path: str, flow_parameters: dict) -> str:
    return path


"""
@s3_sensor is implemented as a flow decorator that's used by customers to ensure 
a workflow only begins after a key in S3 has been written to. 

Example usage:

    def formatter(path: str, flow_parameters: dict) -> str:
        return path.replace("FLOW_ID", flow_parameters["FLOW_ID"])

    @s3_sensor(
        path="s3://aip-example-sandbox/metaflow/S3SensorFlow/data/$date/61/FLOW_ID",
        timeout_seconds=3600, # 1 hour
        polling_interval_seconds=90,
        path_formatter=formatter
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
        "path_formatter": identity_formatter,
    }

    def flow_init(self, flow, graph, environment, datastore, logger, echo, options):
        self.path = self.attributes["path"]
        self.timeout_seconds = self.attributes["timeout_seconds"]
        self.polling_interval_seconds = self.attributes["polling_interval_seconds"]
        self.path_formatter = self.attributes["path_formatter"]

        if not self.path:
            raise MetaflowException("You must specify a S3 path within @s3_sensor.")

        parsed_path = urlparse(self.path)
        if not parsed_path.scheme:
            raise MetaflowException("Please provide a valid S3 path.")

        if not isinstance(self.path_formatter, FunctionType):
            raise MetaflowException("path_formatter must be a function.")
