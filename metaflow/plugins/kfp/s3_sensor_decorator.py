from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

from types import FunctionType
from typing import Tuple

def identity_formatter(key: str, flow_parameters_json: dict) -> str:
    return key

def split_s3_path(path: str) -> Tuple[str, str]:
    path = path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    return bucket, key

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

        try:
            self.bucket, self.key = split_s3_path(self.path)
        except:
            raise MetaflowException(
                "Please specify a valid S3 path. Your path must be "
                "prefixed with s3:// and contain a non-empty key."
            )

        if not isinstance(self.formatter, FunctionType):
            raise MetaflowException("formatter must be a function.")
        