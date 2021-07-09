"""
This function is called within the s3_sensor_op running container.
(1) It decodes the path_formatter function's code and runs it to obtain a final S3 path
(2) It splits the formatted path into an S3 bucket and key 
(3) It polls for an object with the specified bucket and key until timeout
"""
import boto3


def wait_for_s3_path(
    path: str,
    timeout_seconds: int,
    polling_interval_seconds: int,
    path_formatter_code_encoded: str,
    flow_parameters_json: str,
) -> str:
    import boto3
    import botocore
    import base64
    import json
    import marshal
    import time
    from typing import Tuple
    from urllib.parse import urlparse

    flow_parameters = json.loads(flow_parameters_json)
    path_formatter_code = marshal.loads(base64.b64decode(path_formatter_code_encoded))

    def path_formatter_template(key: str, flow_parameters: dict) -> str:
        pass

    path_formatter_template.__code__ = path_formatter_code
    path = path_formatter_template(path, flow_parameters)
    # default variable substitution
    path = path.format(**flow_parameters)
    parsed_path = urlparse(path)
    bucket, key = parsed_path.netloc, parsed_path.path.lstrip("/")

    s3 = boto3.client("s3")
    start_time = time.time()
    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if timeout_seconds is not -1 and elapsed_time > timeout_seconds:
            raise TimeoutError("Timed out while waiting for S3 key..")

        try:
            s3.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            print(".")
        else:
            print(f"Object found at path {path}! Elapsed time: {elapsed_time}.")
            break

        time.sleep(polling_interval_seconds)

    return path
