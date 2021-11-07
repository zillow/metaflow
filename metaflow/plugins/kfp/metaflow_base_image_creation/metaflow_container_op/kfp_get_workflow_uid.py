import argparse
import os

def get_workflow_uid(
    workflow_name: str,
    s3_sensor_path: str,
) -> str:
    """
    The environment variables that this depends on:
        POD_NAMESPACE
    """
    import os
    import subprocess

    command = [
        "kubectl",
        "get",
        "workflow",
        workflow_name,
        "--output",
        "jsonpath='{.metadata.uid}'",
    ]

    namespace = os.environ.get("POD_NAMESPACE", default=None)
    if namespace:
        command.extend(["--namespace", str(namespace)])

    print("command=", " ".join(command))

    result = subprocess.run(command, stdout=subprocess.PIPE)
    uid = result.stdout.decode("utf-8").strip("'")
    print("uid=", uid)
    return uid


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow_name", type=str, required=True)
    parser.add_argument("--s3_sensor_path", type=str, required=True)

    args = parser.parse_args()

    workflow_uid = get_workflow_uid(
        workflow_name=args.workflow_name,
        s3_sensor_path=args.s3_sensor_path,
    )

    output_file = "/tmp/outputs/Output/data"
    try:
        os.makedirs(os.path.dirname(output_file))
    except OSError:
        pass
    with open(output_file, "w") as f:
        f.write(str(workflow_uid))
