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

    If flow_id is a parameter you've passed to your flow, the substitution
    of flow_id in the `path` variable is automatically done for you if you
    specify the variable in braces ({}) (e.g. {flow_id}).

    @s3_sensor(
        path="s3://aip-example-sandbox/metaflow/S3SensorFlow/data/61/{FLOW_ID}",
        timeout_seconds=3600, # 1 hour
        polling_interval_seconds=90,
    )
    class S3SensorFlow(FlowSpec):    
        ...

    If you want to format flow_id (or any other variable in flow_parameters), you
    can do so with a separate `path_formatter` function:

    def formatter(path: str, flow_parameters: dict) -> str:
        path.format(year, flow_parameters["date"].split("-")[-1])

    @s3_sensor(
        path="s3://aip-example-sandbox/metaflow/S3SensorFlow/data/61/{flow_id}/year={year}",
        timeout_seconds=3600, # 1 hour
        polling_interval_seconds=90,
        path_formatter=formatter
    )
    class S3SensorFlow(FlowSpec):    
        ...

    Note, in this example, `flow_parameters` looks like:
    {
        "flow_id": "random_flow_id",
        "date": "07-02-2021"
    }
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

        if not isinstance(self.path_formatter, FunctionType):
            raise MetaflowException("path_formatter must be a function.")