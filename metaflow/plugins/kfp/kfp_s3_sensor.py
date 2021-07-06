def wait_for_s3_path(
    path: str,
    timeout: int,
    polling_interval: int,
    formatter_code_encoded: str,
    flow_parameters_json: str
) -> None:
    import boto3
    import botocore
    import base64
    import marshal
    import json
    import time
    from typing import Tuple

    def split_s3_path(path: str) -> Tuple[str, str]:
        path = path.replace("s3://", "")
        bucket, key = path.split("/", 1)
        return bucket, key

    flow_parameters_json = json.loads(flow_parameters_json)

    formatter_code = marshal.loads(base64.b64decode(formatter_code_encoded))
    def formatter(key: str, flow_parameters_json: dict) -> str:
        pass
    formatter.__code__ = formatter_code
    path = formatter(path, flow_parameters_json)

    bucket, key = split_s3_path(path)

    s3 = boto3.resource('s3')
    start_time = time.time()
    while True:
        try:
            s3.Object(bucket, key).load()
        except botocore.exceptions.ClientError as e:
            print("Object not found. Waiting...")
        else:
            print("Object found! Step complete.")
            break

        current_time = time.time()
        elapsed_time = current_time - start_time
        if timeout is not -1 and elapsed_time > timeout:
            raise Exception("Timed out while waiting for S3 key..")

        time.sleep(polling_interval)
