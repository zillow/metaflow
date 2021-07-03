from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

from types import FunctionType

def identity_formatter(key: str, flow_parameters_json: dict) -> str:
    return key + "at_the_end!"

class S3SensorDecorator(FlowDecorator):
    name = 's3_sensor'
    defaults = {
        "bucket": None,
        "key": "",
        "timeout": -1, # no timeout
        "formatter": identity_formatter
    }

    def flow_init(self, flow, graph,  environment, datastore, logger, echo, options):
        self.bucket = self.attributes["bucket"]
        self.key = self.attributes["key"]
        self.timeout = self.attributes["timeout"]
        self.formatter = self.attributes["formatter"]

        if not self.bucket:
            raise MetaflowException("You must specify a S3 bucket within @s3_sensor.")

        if not self.key:
            raise MetaflowException("You must specify either key or prefix.")

        if not isinstance(self.formatter, FunctionType):
            raise MetaflowException("formatter must be a function.")
        