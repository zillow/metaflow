from metaflow._vendor import click
from metaflow.plugins.aip.aip_decorator import AIPException


@click.group()
def cli():
    pass


@cli.group(name="kfp", help="Deprecated, please use aip instead.")
@click.pass_obj
def kubeflow_pipelines(obj):
    pass


@kubeflow_pipelines.command(help="Deprecated, please use aip instead.")
def run():
    raise AIPException('Deprecated, please use "aip run" instead.')
