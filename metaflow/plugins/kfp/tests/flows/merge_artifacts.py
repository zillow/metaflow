from metaflow import FlowSpec, step, resources

import pytest


class MergeArtifacts(FlowSpec):
    """
    split -> join -> split -> join
    """

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def start(self):
        self.pass_down = "a"
        self.next(self.a, self.b)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def a(self):
        assert self.pass_down == "a"
        self.common = 5
        self.common_2 = 4
        self.x = 1
        self.y = 3
        self.from_a = 6
        self.next(self.join)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def b(self):
        assert self.pass_down == "a"
        self.common = 5
        self.common_2 = 4
        self.x = 2
        self.y = 4
        self.next(self.join)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def join(self, inputs):
        # Ensuring conflicting artifacts must be resolved
        with pytest.raises(AttributeError):
            _ = self.x
        self.x = inputs.a.x
        self.merge_artifacts(inputs, exclude=["y"])
        # Ensuring excluded artifacts are unavailable
        with pytest.raises(AttributeError):
            _ = self.y
        assert self.x == 1
        assert self.pass_down == "a"
        assert self.common == 5
        assert self.from_a == 6
        self.next(self.c)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def c(self):
        self.next(self.d, self.e)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def d(self):
        self.conflicting = 7
        self.next(self.join2)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def e(self):
        self.conflicting = 8
        self.next(self.join2)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def join2(self, inputs):
        assert inputs.d.conflicting == 7
        assert inputs.e.conflicting == 8
        self.merge_artifacts(inputs, include=["pass_down", "common"])
        # Ensuring only included artifacts are available
        with pytest.raises(AttributeError):
            _ = self.common_2
        with pytest.raises(AttributeError):
            _ = self.from_a
        assert self.pass_down == "a"
        assert self.common == 5
        self.next(self.end)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def end(self):
        pass


if __name__ == "__main__":
    MergeArtifacts()
