from typing import Callable

def wait_for_s3_path(
    bucket: str,
    key: str,
    timeout: int,
    formatter_encoded: str,
    flow_parameters_json: str
) -> None:
    import boto3
    import botocore
    import base64
    import marshal
    import pickle
    import json
    import time

    s3 = boto3.resource('s3')
    
    flow_parameters_json = json.loads(flow_parameters_json)
    #formatter = pickle.loads(base64.b64decode(formatter_encoded))
    formatter_code = marshal.loads(base64.b64decode(formatter_encoded))
    def formatter(key: str, flow_parameters_json: dict) -> str:
        pass
    formatter.__code__ = formatter_code

    #key = formatter(key, flow_parameters_json)

    print("key: ", key)
    print("flow_parameters_json: ", flow_parameters_json)
    print("type(flow_parameters_json): ", type(flow_parameters_json))
    print("type(formatter): ", formatter)

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
            raise Exception("Timed out while waiting for S3 key or prefixed path..")

        time.sleep(1)
