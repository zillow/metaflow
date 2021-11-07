"""
This function is called within the s3_sensor_op running container.
(1) It decodes the path_formatter function's code and runs it to obtain a final S3 path
(2) It splits the formatted path into an S3 bucket and key 
(3) It polls for an object with the specified bucket and key until timeout
"""
import argparse
import os

def wait_for_s3_path(
    path: str,
    timeout_seconds: int,
    polling_interval_seconds: int,
    path_formatter_code_encoded: str,
    flow_parameters_json: str,
    os_expandvars: bool,
) -> str:
    import boto3
    import botocore
    import base64
    import json
    import marshal
    import time
    from urllib.parse import urlparse
    import os

    if flow_parameters_json:
        flow_parameters = json.loads(flow_parameters_json)
    else:
        flow_parameters = {}

    if path_formatter_code_encoded:
        path_formatter_code = marshal.loads(
            base64.b64decode(path_formatter_code_encoded)
        )

        def path_formatter_template(key: str, flow_parameters: dict) -> str:
            pass

        path_formatter_template.__code__ = path_formatter_code
        path = path_formatter_template(path, flow_parameters)
    else:
        if os_expandvars:
            # expand OS env variables
            path = os.path.expandvars(path)
        # default variable substitution
        path = path.format(**flow_parameters)

    # debugging print statement for customers so they know the final path
    # we're looking for
    print(f"Waiting for path: {path}...")

    parsed_path = urlparse(path)
    bucket, key = parsed_path.netloc, parsed_path.path.lstrip("/")

    s3 = boto3.client("s3")
    start_time = time.time()
    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time > timeout_seconds:
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, required=True)
    parser.add_argument("--timeout_seconds", type=int, required=True)
    parser.add_argument("--polling_interval_seconds", type=int, required=True)
    parser.add_argument("--path_formatter_code_encoded", type=str, default=None, required=False)
    parser.add_argument("--flow_parameters_json", type=str, default=None, required=False)
    parser.add_argument("--os_expandvars", action="store_true")

    args = parser.parse_args()

    s3_path = wait_for_s3_path(
        path=args.path,
        timeout_seconds=args.timeout_seconds,
        polling_interval_seconds=args.polling_interval_seconds,
        path_formatter_code_encoded=args.path_formatter_code_encoded,
        flow_parameters_json=args.flow_parameters_json,
        os_expandvars=args.os_expandvars,
    )

    output_file = "/tmp/outputs/Output/data"
    try:
        os.makedirs(os.path.dirname(output_file))
    except OSError:
        pass
    with open(output_file, "w") as f:
        f.write(str(s3_path))
