from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException


class KfpStepDecorator(StepDecorator):
    """
    For KFP orchestrator plugin only.

    Step decorator to specify the image for the step.

    To use, follow the example below.
    ```
    @kfp_step(image="image_on_dockerhub")
    @step
    def start(self):
        self.next(self.next_step)

    ```

    Parameters
    ----------
    image: str
        Defaults to None, which means default to base image.
        Must resolve to an actual image, either publicly hosted
        or available for download on the customer's infra.
    """

    name = "kfp_step"

    defaults = {
        "image": None,
    }
