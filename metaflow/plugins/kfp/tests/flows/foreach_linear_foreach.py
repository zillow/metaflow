from metaflow import FlowSpec, step, resources


class ForeachLinearForeach(FlowSpec):
    """
    foreach -> linear -> linear -> foreach -> linear -> linear -> join
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
        self.next(self.linear_2)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def linear_2(self):
        self.next(self.foreach_split_z)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_split_z(self):
        self.z = "ef"
        self.next(self.linear_3, foreach="z")

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def linear_3(self):
        self.next(self.linear_4)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def linear_4(self):
        self.next(self.foreach_join_z)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_join_z(self, inputs):
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
    ForeachLinearForeach()
