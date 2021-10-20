from metaflow import FlowSpec, accelerator, resources, step


class TolerationAndAffinityFlow(FlowSpec):
    @accelerator(type="nvidia-tesla-v100")
    @resources(
        local_storage="242",
        cpu="0.6",
        memory="2G",
    )
    @step
    def start(self):
        print("This step simulates usage of a nvidia-tesla-v100 GPU.")
        self.next(self.small_cpu_pod)

    @resources(cpu=4)
    @step
    def small_cpu_pod(self):
        """Expect taint for medium node type"""
        self.next(self.small_memory_pod)

    @resources(memory="32Gi")
    @step
    def small_memory_pod(self):
        """Expect taint for medium node type"""
        self.next(self.large_cpu_pod)

    @resources(cpu=32.0, memory=5)
    @step
    def large_cpu_pod(self):
        """Expect taint for large node type"""
        self.next(self.large_memory_pod)

    # 128000m ~= 128G memory - testing different resource request formats
    @resources(memory=128000, cpu=1)
    @step
    def large_memory_pod(self):
        """Expect taint for large node type"""
        self.next(self.large_memory_cpu_pod)

    # 300 Gi, 50.5 CPU - testing different resource request formats
    @resources(memory="300Gi", cpu="50500m")
    @step
    def large_memory_cpu_pod(self):
        """Expect taint for large node type"""
        self.next(self.end)

    @step
    def end(self):
        print("All done.")


if __name__ == "__main__":
    TolerationAndAffinityFlow()
