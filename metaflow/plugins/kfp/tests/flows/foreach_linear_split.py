from metaflow import FlowSpec, step, resources


class ForeachLinearSplit(FlowSpec):
    """
    foreach -> linear -> split -> linear -> foreach -> join
    """

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def start(self):
        self.x = "ab"
        self.next(self.linear_1, foreach="x")

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def linear_1(self):
        self.next(self.split_a_and_b)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def split_a_and_b(self):
        self.next(self.a, self.b)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def a(self):
        self.next(self.foreach_split_a)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def b(self):
        self.next(self.foreach_split_b)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_split_a(self):
        self.z = "ef"
        self.next(self.linear_2, foreach="z")

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_split_b(self):
        self.z = "ef"
        self.next(self.linear_3, foreach="z")

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def linear_2(self):
        self.next(self.foreach_join_a)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def linear_3(self):
        self.next(self.foreach_join_b)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_join_a(self, inputs):
        self.next(self.join_a_and_b)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_join_b(self, inputs):
        self.next(self.join_a_and_b)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def join_a_and_b(self, inputs):
        self.next(self.foreach_join_start)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_join_start(self, inputs):
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
    ForeachLinearSplit()
