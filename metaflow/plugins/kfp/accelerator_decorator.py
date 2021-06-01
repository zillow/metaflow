from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException


class AcceleratorDecorator(StepDecorator):
    """
    For KFP orchestrator plugin only.

    Step decorator to specify type of hardware accelerator used in a step.

    To use, follow the example below. 
    ```
    @step
    def start(self):
        self.ranks = list(range(self.world_size))
        print(f"ranks: {self.ranks}")
        self.next(self.train, foreach="ranks")

    @pytorch_distributed
    @accelerator
    @step
    def train(self):
        self.rank = self.input
        # pytorch code
        ...
    ```

    Parameters
    ----------
    accelerator_type: str
        Defaults to nvidia-tesla-v100.
        More GPUs will be added based on customer needs.
    required: bool
        Defaults to true.
        Whether your step requires this accelerator, or simply prefers it.
    """

    name = "accelerator"

    defaults = {
        "accelerator_type": "nvidia-tesla-v100",
        "accelerator_required": True
    }

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        pass
