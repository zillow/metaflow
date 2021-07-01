from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

class S3SensorDecorator(FlowDecorator):
    name = 's3_sensor'
    defaults = {
        "bucket": None,
        "key": "",
        "prefix": "",
        "timeout": -1 # no timeout
    }

    def flow_init(self, flow, graph,  environment, datastore, logger, echo, options):
        self.bucket = self.attributes["bucket"]
        self.key = self.attributes["key"]
        self.prefix = self.attributes["prefix"]
        self.timeout = self.attributes["timeout"]

        if not self.path:
            raise MetaflowException("You must specify a S3 path within @s3_sensor.")

        if not self.key and not self.prefix:
            raise MetaflowException("You must specify either key or prefix.")
