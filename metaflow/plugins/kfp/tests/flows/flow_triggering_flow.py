from metaflow import FlowSpec, step, Parameter, current

try:
    import kfp
except:
    import os

    os.system(
        "pip install -i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple/ zillow-kfp"
    )

from metaflow.plugins.kfp.kfp_utils import (
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
        if self.triggered_by:
            print(f"This flow is triggered by run {self.triggered_by}")

        if self.trigger_enabled:
            print("Triggering Downstream Flow...")
            run = run_kubeflow_pipeline(
                kubeflow_pipeline_name="FlowTriggeringFlow",
                kubeflow_experiment_name="default",
                kubeflow_namespace=self.triggered_flow_namespace,
                triggered_run_name=f"FlowTriggeringFlow triggered by run {current.run_id}",
                parameters={
                    "triggered_by": current.run_id,
                },
            )
            print("Run ID:", run.id)
            print("Run URL:", run_id_to_url(run.id))

            wait_for_kfp_run_completion(run.id, wait_timeout=180)

        self.next(self.end)

        # TODO: test timeout behavior

    @step
    def end(self):
        pass


if __name__ == "__main__":
    FlowTriggeringFlow()
