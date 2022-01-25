import os
import time

from metaflow import FlowSpec, Parameter, current, step

os.system(
    "pip install --quiet --disable-pip-version-check "
    "-i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple/ "
    "zillow-kfp kfp-server-api"
)
from metaflow.plugins.kfp import (  # noqa
    get_kfp_run,
    run_id_to_url,
    run_kubeflow_pipeline,
    wait_for_kfp_run_completion,
)
from metaflow.plugins.kfp.kfp_utils import _get_kfp_client  # noqa


TEST_PIPELINE_NAME = "metaflow-unit-test-flow-triggering-flow"


class FlowTriggeringFlow(FlowSpec):
    trigger_enabled = Parameter("trigger_enabled", default=False)
    triggered_by = Parameter(name="triggered_by", default=None)
    triggered_flow_namespace = Parameter(
        name="namespace", default="aip-metaflow-sandbox"
    )

    @step
    def start(self):
        if self.triggered_by:
            print(f"This flow is triggered by run {self.triggered_by}")

        if self.trigger_enabled:  # Upload pipeline

            def _self_upload_as_pipeline():
                """Upload this flow to keep version consistency

                Flow triggering flow only triggers uploaded flows.
                This function is a workaround to unit test on consistent downstream pipeline
                version.

                Warning: This function is not recommended for production usage
                    Users are recommended to upload pipeline though CICD to take advantage of
                    testing
                """
                print("Uploading downstream pipeline for test")

                import tempfile
                from datetime import datetime

                import kfp_server_api

                with tempfile.TemporaryDirectory() as dir_path:
                    pipeline_file_path = f"{dir_path}/pipeline.yaml"
                    print(f"Compiling test flow to local file {pipeline_file_path}...")
                    os.system(
                        f"python '{__file__}' kfp run --yaml-only --pipeline-path '"
                        f"{pipeline_file_path}'"
                    )

                    print("Uploading pipeline...")
                    client = _get_kfp_client()
                    try:
                        pipeline: kfp_server_api.ApiPipeline = client.upload_pipeline(
                            pipeline_package_path=pipeline_file_path,
                            pipeline_name=TEST_PIPELINE_NAME,
                        )
                        self.pipeline_id = pipeline.id
                        self.create_at = pipeline.created_at
                        self.version_id = pipeline.default_version.id
                    except kfp_server_api.exceptions.ApiException:
                        version: kfp_server_api.ApiPipelineVersion = client.upload_pipeline_version(
                            pipeline_package_path=pipeline_file_path,
                            pipeline_version_name=f"flow_triggering_flow_{datetime.now()}",
                            pipeline_name=TEST_PIPELINE_NAME,
                        )
                        self.pipeline_id = version.resource_references[0].key.id
                        self.version_id = version.id
                        self.create_at = version.created_at
                    print(f"Uploaded test pipeline version {self.version_id}.")

            _self_upload_as_pipeline()
        self.next(self.test_trigger_and_wait)

    @step
    def test_trigger_and_wait(self):
        if self.trigger_enabled:
            # Workaround: Base image used for testing may not have `zillow-kfp` or `kfp-server-api`
            # Actual user should have these packages installed in their base image

            print("Triggering Downstream Flow...")
            run = run_kubeflow_pipeline(
                pipeline_name=TEST_PIPELINE_NAME,
                kubeflow_namespace=self.triggered_flow_namespace,
                triggered_run_name=f"FlowTriggeringFlow triggered by run {current.run_id}",
                kubeflow_experiment_name="default",
                parameters={
                    "triggered_by": current.run_id,
                },
                # Specify version so that multiple instance of tests can be triggered
                pipeline_version_id=self.version_id,
            )

            print("Run ID:", run.id)
            print("Run URL:", run_id_to_url(run.id))

            time.sleep(10)
            run = get_kfp_run(run.id)
            print(run.status)

            run = wait_for_kfp_run_completion(run_id=run.id, wait_timeout=180)
            print(run.status)

        self.next(self.end)

    @step
    def end(self):
        if self.trigger_enabled:
            print(f"Deleting vesrion {self.version_id}")
            client = _get_kfp_client()
            client.delete_pipeline_version(self.version_id)


if __name__ == "__main__":
    FlowTriggeringFlow()
