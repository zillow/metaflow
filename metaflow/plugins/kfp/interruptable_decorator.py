from metaflow.decorators import StepDecorator


class InterruptableDecorator(StepDecorator):
    """
    For KFP orchestrator plugin only.

    Step decorator to specify that the pod be can be interrupted (ex: Spot, pod consolidation, etc)

    To use, follow the example below.
    ```
    @interruptable()
    @step
    def train(self):
        self.rank = self.input
        # code running on interruptable instance
        ...
    ```

    Parameters
    ----------
    """

    name = "interruptable"
