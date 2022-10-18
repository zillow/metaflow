from metaflow.decorators import StepDecorator



class SpotDecorator(StepDecorator):
    """
    For KFP orchestrator plugin only.

    Step decorator to specify that the pod be on a Spot node.

    To use, follow the example below.
    ```
    @spot()
    @step
    def train(self):
        self.rank = self.input
        # code running on spot instance
        ...
    ```

    Parameters
    ----------
    """

    name = "spot"