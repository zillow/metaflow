import posixpath
from distutils.version import LooseVersion

import click

from metaflow import current, decorators
from metaflow.datastore.datastore import TransformableObject
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    KFP_SDK_API_NAMESPACE,
    KFP_SDK_NAMESPACE,
    KFP_RUN_URL_PREFIX,
)
from metaflow.package import MetaflowPackage
from metaflow.plugins.kfp.constants import (
    DEFAULT_EXPERIMENT_NAME,
    DEFAULT_FLOW_CODE_URL,
    DEFAULT_RUN_NAME,
    DEFAULT_KFP_YAML_OUTPUT_PATH,
)
from metaflow.plugins.kfp.kfp import KubeflowPipelines
from metaflow.plugins.kfp.kfp_decorator import KfpInternalDecorator
from metaflow.util import get_username


class IncorrectMetadataServiceVersion(MetaflowException):
    headline = "Incorrect version for metaflow service"


@click.group()
def cli():
    pass


@cli.group(name="kfp", help="Commands related to Kubeflow Pipelines.")
@click.pass_obj
def kubeflow_pipelines(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)


@kubeflow_pipelines.command(help="Deploy a new version of this workflow to Kubeflow Pipelines.")
@click.option(
    "--code-url",
    "code_url",
    default=DEFAULT_FLOW_CODE_URL,
    help="the code URL of the flow to be executed on KFP",
)
@click.option(
    "--experiment-name",
    "experiment_name",
    default=DEFAULT_EXPERIMENT_NAME,
    help="the associated experiment name for the run",
)
@click.option(
    "--run-name", "run_name", default=DEFAULT_RUN_NAME, help="name assigned to the new KFP run",
)
@click.option(
    "--namespace", "namespace", default=KFP_SDK_NAMESPACE, help="namespace of your run in KFP.",
)
@click.option(
    "--api-namespace",
    "api_namespace",
    default=KFP_SDK_API_NAMESPACE,
    help="namespace where the API service is run.",
)
@click.option(
    "--yaml-only",
    "yaml_only",
    is_flag=True,
    default=False,
    help="Generate the KFP YAML which is used to run the workflow on Kubeflow Pipelines.",
)
@click.option(
    "--pipeline-path",
    "pipeline_path",
    default=DEFAULT_KFP_YAML_OUTPUT_PATH,
    help="the output path (or filename) of the generated KFP pipeline yaml file",
)
@click.pass_obj
def run(
    obj,
    code_url=DEFAULT_FLOW_CODE_URL,
    experiment_name=DEFAULT_EXPERIMENT_NAME,
    run_name=DEFAULT_RUN_NAME,
    namespace=KFP_SDK_NAMESPACE,
    api_namespace=KFP_SDK_API_NAMESPACE,
    yaml_only=False,
    pipeline_path=DEFAULT_KFP_YAML_OUTPUT_PATH,
):
    check_metadata_service_version(obj)
    flow = make_flow(obj, current.flow_name, namespace, api_namespace)

    if yaml_only:
        pipeline_path = flow.create_kfp_pipeline_yaml(code_url, pipeline_path)
        obj.echo(
            "\nDone converting *{name}* to {path}".format(
                name=current.flow_name, path=pipeline_path
            )
        )
    else:
        obj.echo("Deploying *%s* to Kubeflow Pipelines..." % current.flow_name, bold=True)
        run_pipeline_result = flow.create_run_on_kfp(
            code_url, experiment_name, run_name, namespace, api_namespace, get_username()
        )

        obj.echo("\nRun created successfully!\n")
        kfp_run_url = posixpath.join(
            KFP_RUN_URL_PREFIX, "_/pipeline/#/runs/details", run_pipeline_result.run_id
        )
        obj.echo("Workflow *{name}* triggered on KFP".format(name=current.flow_name), bold=True)
        obj.echo("{kfp_run_url}".format(kfp_run_url=kfp_run_url), fg="cyan")


def check_metadata_service_version(obj):
    metadata = obj.metadata
    version = metadata.version()
    if version == "local":
        return
    elif version is not None and LooseVersion(version) >= LooseVersion("2.0.2"):
        # Metaflow metadata service needs to be at least at version 2.0.2
        return
    else:
        obj.echo("")
        obj.echo(
            "You are running a version of the metaflow service "
            "that currently doesn't support Kubeflow Pipelines. "
        )
        obj.echo(
            "For more information on how to upgrade your "
            "service to a compatible version (>= 2.0.2), visit:"
        )
        obj.echo(
            "    https://admin-docs.metaflow.org/metaflow-on-aws/operation"
            "s-guide/metaflow-service-migration-guide",
            fg="green",
        )
        obj.echo("Once you have upgraded your metadata service, please " "re-execute your command.")
        raise IncorrectMetadataServiceVersion(
            "Try again with a more recent " "version of metaflow service " "(>=2.0.2)."
        )


def make_flow(obj, name, namespace, api_namespace):
    datastore = obj.datastore(
        obj.flow.name,
        mode="w",
        metadata=obj.metadata,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
    )

    if datastore.TYPE != "s3":
        raise MetaflowException("Kubeflow Pipelines requires --datastore=s3.")

    # Attach KFP decorator to the flow
    decorators._attach_decorators(obj.flow, [KfpInternalDecorator.name])
    decorators._init_decorators(obj.flow, obj.graph, obj.environment, obj.datastore, obj.logger)

    obj.package = MetaflowPackage(obj.flow, obj.environment, obj.logger, obj.package_suffixes)
    package_url = datastore.save_data(obj.package.sha, TransformableObject(obj.package.blob))

    return KubeflowPipelines(
        name,
        obj.graph,
        obj.flow,
        obj.package,
        package_url,
        obj.metadata,
        obj.datastore,
        obj.environment,
        obj.event_logger,
        obj.monitor,
        namespace=namespace,
        api_namespace=api_namespace,
        username=get_username(),
    )
