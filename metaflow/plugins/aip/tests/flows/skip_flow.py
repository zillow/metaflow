from metaflow import Parameter, FlowSpec, step, skip


class SkipFlow(FlowSpec):

    condition_true = Parameter("condition-true", default=True)

    @step
    def start(self):
        print("Should skip:", self.condition)
        self.desired_step_executed = False
        self.condition_false = False
        self.next(self.skipped_step)

    @skip(check="condition_true", next="desired_step")
    @step
    def skipped_step(self):
        raise Exception(
            "Unexpectedly ran the skipped_step step. This step should have been skipped."
        )
        self.next(self.unreachable)

    def unreachable(self):
        raise Exception(
            "Unexpectedly ran the unreachable step. This step should have been skipped."
        )
        self.next(self.end)

    @skip(check="condition_false", next="end")
    @step
    def desired_step(self):
        self.desired_step_executed = True
        self.next(self.end)

    @step
    def end(self):
        assert self.desired_step_executed, "Desired step was not executed"


if __name__ == "__main__":
    SkipFlow()
