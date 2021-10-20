from metaflow import FlowSpec, step, environment, resources
import yaml


class CompileTimeValidationFlow(FlowSpec):
    """This flow is only intended for compile time behavior

    This flow targets 3 machine type and test that for each type the taints are correctly added:
    c5.4xlarge: 16 vCPU, 32 GB)
    m5.8xlarge: 32 vCPU, 128 GB)
    r5.12xlarge: 48 vCPU, 384 GB)
    """

    @step
    def start(self):
        """Should not have taint"""
        self.next(self.medium_cpu_pod)

    @step
    @resources(cpu=16)
    def medium_cpu_pod(self):
        """Expect taint for medium node type"""
        self.next(self.large_cpu_pod)

    @step
    @resources(cpu=32.0)
    def large_cpu_pod(self):
        """Expect taint for large node type"""
        self.next(self.medium_memory_pod)

    @step
    @resources(memory="32G")
    def medium_memory_pod(self):
        """Expect taint for medium node type"""
        self.next(self.large_memory_pod)

    @step
    @resources(memory="128G")
    def large_memory_pod(self):
        """Expect taint for large node type"""
        self.next(self.end)

    @step
    def end(self):
        self.next(self.small_pod)


def test_resource_default_node_type_taint(tmp_path):
    """Test node type taints added by default based on resource requirement"""
    flow = CompileTimeValidationFlow(use_cli=False)
    pipeline_path = flow.create_kfp_pipeline_yaml(tmp_path / f"{flow.name}.yml")
    print(f"=== Pipeline Path: {pipeline_path} ===")

    with open(pipeline_path, "r") as pipeline_yml:
        pipeline_spec = yaml.parse(pipeline_yml)
        print(pipeline_spec)
