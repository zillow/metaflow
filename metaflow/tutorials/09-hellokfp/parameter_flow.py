from metaflow import FlowSpec, Parameter, step


class ParameterFlow(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    The hello step uses @resource decorator that only works when kfp plug-in is used.
    """

    alpha = Parameter(
        'alpha',
        help='param with default',
        default=0.01,
    )

    beta = Parameter(
        'beta',
        help='param with no default',
        type=int,
        required=True
    )

    def __init__(self):
        super(ParameterFlow, self).__init__()

    @step
    def start(self):
        """
        All flows must have a step named 'start' that is the first step in the flow.
        """
        print(f"Alpha: {self.alpha}")
        print(f"Beta: {self.beta}")
        self.next(self.end)

    @step
    def end(self):
        """
        All flows must have an 'end' step, which is the last step in the flow.
        """
        print(f"Alpha: {self.alpha}")
        print(f"Beta: {self.beta}")


if __name__ == '__main__':
    ParameterFlow()
