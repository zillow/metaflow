from metaflow.plugins.kfp.kfp_s3_sensor import wait_for_s3_path

from unittest.mock import call, Mock, patch, PropertyMock
import pytest

import boto3
from botocore.exceptions import ClientError

import base64
import marshal

"""
To run these tests from your terminal, go to the root directory and run:

`python -m pytest metaflow/plugins/kfp/tests/test_kfp_s3_sensor.py -c /dev/null`

The `-c` flag above tells PyTest to ignore the setup.cfg config file which is used
for the integration tests.

"""


def identity_formatter(path: str, flow_parameters: dict) -> str:
    return path


# This test ensures a timeout exception is raised when wait_for_s3_path
# looks for a nonexistent S3 path. This ensures we don't have an idle
# pod using up resources continuously.
def test_wait_for_s3_path_timeout_exception():
    identity_formatter_code_encoded = base64.b64encode(
        marshal.dumps(identity_formatter.__code__)
    ).decode("ascii")

    with pytest.raises(TimeoutError):
        wait_for_s3_path(
            path="s3://sample_bucket/sample_prefix/sample_key",
            timeout_seconds=1,
            polling_interval_seconds=1,
            path_formatter_code_encoded=identity_formatter_code_encoded,
            flow_parameters_json='{"key": "value"}',
        )
