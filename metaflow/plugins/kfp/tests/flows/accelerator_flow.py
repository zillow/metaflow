import os

from metaflow import FlowSpec, step, resources, accelerator


class AcceleratorFlow(FlowSpec):
    @accelerator(type="nvidia-tesla-v100")
    @step
    def start(self):
        print("This step simulates usage of a nvidia-tesla-v100 GPU.")
        self.next(self.end)

    @step
    def end(self):
        print("All done.")


if __name__ == "__main__":
    AcceleratorFlow()
