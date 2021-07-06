from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

from types import FunctionType
from typing import Tuple

def identity_formatter(path: str, flow_parameters_json: dict) -> str:
    return path

class S3SensorDecorator(FlowDecorator):
    name = 's3_sensor'
    defaults = {
        "path": "",
        "timeout": 3600,
        "polling_interval": 300,
        "formatter": identity_formatter
    }

    def flow_init(self, flow, graph,  environment, datastore, logger, echo, options):
        self.path = self.attributes["path"]
        self.timeout = self.attributes["timeout"]
        self.polling_interval = self.attributes["polling_interval"]
        self.formatter = self.attributes["formatter"]

        if not self.path:
            raise MetaflowException("You must specify a S3 path within @s3_sensor.")

        if not self.path.startswith("s3://"):
            raise MetaflowException("Your path must be prefixed with s3://")
        
        prefix_removed_path = self.path.replace("s3://", 1)
        if len(prefix_removed_path.split("/", 1)) == 1:
            raise MetaflowException("Your path must contain both a bucket and key, separated by a slash.")

        if not isinstance(self.formatter, FunctionType):
            raise MetaflowException("formatter must be a function.")
        