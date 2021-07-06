from metaflow.decorators import FlowDecorator
from metaflow.exception import MetaflowException

from types import FunctionType
from typing import Tuple

"""
Within identity_formatter, which is passed in as the func parameter,
customers have access to all variables in flow_parameters_json (which 
include all parameters passed in through the flow at compile and run time by
KFP) as well as a number of environment variables, including:

MF_POD_NAME
MF_POD_NAMESPACE
MF_ARGO_NODE_NAME
MF_ARGO_WORKFLOW_NAME
ZODIAC_SERVICE
ZODIAC_TEAM
POD_NAMESPACE
KFP_SDK_NAMESPACE
POD_NAME
POD_IP
ARGO_WORKFLOW_NAME
K8S_CLUSTER_NAME
K8S_CLUSTER_ENV
METAFLOW_NOTIFY_EMAIL_SMTP_HOST
METAFLOW_NOTIFY_EMAIL_SMTP_PORT
METAFLOW_NOTIFY_EMAIL_FROM
METAFLOW_NOTIFY_EMAIL_BODY
METAFLOW_NOTIFY_ON_ERROR
KFP_RUN_URL_PREFIX
METAFLOW_DEFAULT_DATASTORE
METAFLOW_DEFAULT_METADATA
METAFLOW_SERVICE_URL
METAFLOW_DATASTORE_SYSROOT_S3
WAGGLE_DANCE_DEFAULT_ENVIRONMENT
WAGGLE_DANCE_DEV_ENDPOINT
WAGGLE_DANCE_STAGE_ENDPOINT
WAGGLE_DANCE_PROD_READ_ONLY_ENDPOINT
WAGGLE_DANCE_PROD_ENDPOINT
AWS_ROLE_ARN
AWS_WEB_IDENTITY_TOKEN_FILE
"""


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
        "func": identity_formatter,
    }

    def flow_init(self, flow, graph, environment, datastore, logger, echo, options):
        self.path = self.attributes["path"]
        self.timeout_seconds = self.attributes["timeout_seconds"]
        self.polling_interval_seconds = self.attributes["polling_interval_seconds"]
        self.func = self.attributes["func"]

        if not self.path:
            raise MetaflowException("You must specify a S3 path within @s3_sensor.")

        if not self.path.startswith("s3://"):
            raise MetaflowException("Your path must be prefixed with s3://")

        prefix_removed_path = self.path.replace("s3://", "", 1)
        if len(prefix_removed_path.split("/", 1)) == 1:
            raise MetaflowException(
                "Your path must contain both a bucket and key, separated by a slash."
            )

        if not isinstance(self.func, FunctionType):
            raise MetaflowException("formatter must be a function.")
