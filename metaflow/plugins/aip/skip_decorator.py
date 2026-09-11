# Skip decorator is a workaround solution to implement conditional branching in metaflow.
# When condition variable is_skipping is evaluated to True,
# it will skip current step and execute the supplied next step.

from functools import wraps
from metaflow.decorators import StepDecorator


class SkipDecorator(StepDecorator):
    """
    The @skip decorator is a workaround for conditional branching. The @skip decorator checks an artifact
    and if it is false, skips the evaluation of the step function and jumps to the supplied next step.

    **The `start` and `end` steps are always expected and should not be skipped.**

    Usage:
        class SkipFlow(FlowSpec):

        condition = Parameter("condition", default=False)

        @step
        def start(self):
            print("Should skip:", self.condition)
            self.next(self.middle)

        @skip(check='condition', next='end')
        @step
        def middle(self):
            print("Running the middle step - not skipping")
            self.next(self.end)

        @step
        def end(self):
            pass
    """

    name = "skip"

    def __init__(self, check="", next=""):
        super().__init__()
        self.check = check
        self.next = next

    def __call__(self, f):
        @wraps(f)
        def func(step):
            if getattr(step, self.check):
                step.next(getattr(step, self.next))
            else:
                return f(step)

        return func
