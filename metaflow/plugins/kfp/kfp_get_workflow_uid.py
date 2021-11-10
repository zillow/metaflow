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
    # from kubernetes.client import CustomObjectsApi

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

    # print("command=", " ".join(command))

    # GROUP = "argoproj.io"
    # VERSION = "v1alpha1"
    # PLURAL = "workflows"
    # NAMESPACE = namespace if namespace else None

    # crd_api = CustomObjectsApi()
    # workflow = crd_api.get_namespaced_custom_object(GROUP, VERSION, NAMESPACE, PLURAL, workflow_name)

    # print("workflow: ", workflow)

    result = subprocess.run(command, stdout=subprocess.PIPE)
    uid = result.stdout.decode("utf-8").strip("'")
    print("uid=", uid)
    return uid
