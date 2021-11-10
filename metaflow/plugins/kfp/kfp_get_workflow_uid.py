import click


@click.command()
@click.option("--workflow_name")
@click.option("--s3_sensor_path")
def get_workflow_uid(
    workflow_name: str,
    # see https://zbrt.atl.zillow.net/browse/AIP-5404
    # for explanation of s3_sensor_path parameter
    s3_sensor_path: str,
) -> str:
    """
    The environment variables that this depends on:
        POD_NAMESPACE
    """
    import os
    from kubernetes import config, dynamic
    from kubernetes.client import api_client

    namespace = os.environ.get("POD_NAMESPACE", default=None)

    configuration = config.load_incluster_config()
    dynamic_client = dynamic.DynamicClient(
        api_client.ApiClient(configuration=configuration)
    )
    workflow_api = dynamic_client.resources.get(
        api_version="argoproj.io/v1alpha1", kind="Workflow"
    )
    if namespace:
        workflow = workflow_api.get(
            name=workflow_name,
            namespace=namespace,
        )
    else:
        workflow = workflow_api.get(
            name=workflow_name,
        )

    uid = workflow["metadata"]["uid"]
    print("uid=", uid)
    
    output_file = "/tmp/outputs/Output/data"
    try:
        os.makedirs(os.path.dirname(output_file))
    except OSError:
        pass
    with open(output_file, "w") as f:
        f.write(str(uid))

if __name__ == "__main__":
    get_workflow_uid()
