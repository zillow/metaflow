from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

class S3SensorDecorator(FlowDecorator):
    name = 's3_sensor'
    defaults = {
        "path": None,
        "timeout": -1 # no timeout
    }

    def flow_init(self, flow, graph,  environment, datastore, logger, echo, options):
        self.path = self.attributes["path"]
        self.timeout = self.attributes["timeout"]

        if not self.path:
            raise MetaflowException("You must specify a S3 path within @s3_sensor.")
