import os

from metaflow import FlowSpec, step, Parameter, current
from metaflow.plugins.kfp import (
    get_kfp_run,
    run_id_to_url,
    run_kubeflow_pipeline,
    wait_for_kfp_run_completion,
)


class FlowTriggeringFlow(FlowSpec):
    trigger_enabled = Parameter("trigger_enabled", default=False)
    triggered_by = Parameter(name="triggered_by", default=None)
    triggered_flow_namespace = Parameter(
        name="namespace", default="aip-metaflow-sandbox"
    )

    @step
    def start(self):
        os.system(
            "pip install "
            "-i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple/ "
            "zillow-kfp kfp-server-api"
        )

        if self.triggered_by:
            print(f"This flow is triggered by run {self.triggered_by}")

        if self.trigger_enabled:
            print("Triggering Downstream Flow...")
            run = run_kubeflow_pipeline(
                pipeline_name="FlowTriggeringFlow",
                kubeflow_namespace=self.triggered_flow_namespace,
                triggered_run_name=f"FlowTriggeringFlow triggered by run {current.run_id}",
                kubeflow_experiment_name="default",
                parameters={
                    "triggered_by": current.run_id,
                },
            )
            print("Run ID:", run.id)
            print("Run URL:", run_id_to_url(run.id))

            run = get_kfp_run(run.id)
            print(run.status)

            run = wait_for_kfp_run_completion(run.id, wait_timeout=180)
            print(run.status)

        self.next(self.end)

        # TODO: test timeout behavior

    @step
    def end(self):
        pass


if __name__ == "__main__":
    FlowTriggeringFlow()
