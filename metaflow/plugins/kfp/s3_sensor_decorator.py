from metaflow.decorators import FlowDecorator


class S3SensorDecorator(FlowDecorator):
    name = 's3_sensor'
    defaults = {
        "path": None,
        "timeout": -1 # no timeout
    }

    def flow_init(self, flow, graph,  environment, datastore, logger, echo, options):
        self.path = self.attributes["path"]
        self.timeout = self.attributes["timeout"]
