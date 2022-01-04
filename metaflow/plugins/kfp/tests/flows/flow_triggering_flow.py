from metaflow import FlowSpec, step, Parameter, current

try:
    import kfp
except:
    import os

    os.system(
        "pip install -i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple/ zillow-kfp"
    )
from metaflow.plugins.kfp.kfp_cli import trigger_flow, run_id_to_url


class FlowTriggeringFlow(FlowSpec):
    triggered_by = Parameter(name="triggered", default=None)
    triggered_flow_namespace = Parameter(name="triggered", default="aip-metaflow-sandbox")

    @step
    def start(self):
        if not self.triggered_by:
            print("Triggering Downstream Flow...")
            run_id = trigger_flow(
                pipeline_name="FlowTriggeringFlow",
                experiment_name="default",
                namespace=self.triggered_flow_namespace,
                triggerred_flow_name="Flow triggered by upstream",
                pipeline_parameters={"triggered_by": current.run_id},
            )
            print("Run ID:", run_id)
            print("Run URL:", run_id_to_url(run_id))
        else:
            print(f"This flow is triggered by run {self.triggered_by}")

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    FlowTriggeringFlow()
