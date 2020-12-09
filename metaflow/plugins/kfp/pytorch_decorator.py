from metaflow.decorators import StepDecorator


class PyTorchDecorator(StepDecorator):
    """
    For KFP orchestrator plugin only.

    Step decorator to specify that the parallel node is a pytorch cluster.

    To use, annotate your step as follows:
    ```
    @step
    def start(self):
        self.ranks = list(range(self.world_size))
        print(f"ranks: {self.ranks}")
        self.next(self.train, foreach="ranks")

    @pytorch
    @step
    def train(self):
        self.rank = self.input
        # pytorch code
        ...
    ```

    Parameters
    ----------
    shared_volume_size : str
        Shared volume size limit. Default unit is MB.
            Other units are supported, including "E", "P", "T", "G", "M", "K". (i.e. "4000M")
            Defaults 100M
    shared_volume_dir : str
        Where to mount the shared volume.
        Defaults to: /opt/pytorch_shared
    """

    name = "pytorch"

    defaults = {
        # KFP supported attributes
        "shared_volume_size": "100M",
        "shared_volume_dir": "/opt/pytorch_shared/"
        # "mode": "shared_volume"  # Coming soon: support for RPC
    }
