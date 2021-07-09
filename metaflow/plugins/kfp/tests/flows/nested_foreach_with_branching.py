from metaflow import FlowSpec, step, resources


class NestedForeachWithBranching(FlowSpec):
    """
    split -> foreach -> foreach -> foreach -> linear -> linear -> join -> join -> join -> join (with below split)
          -> split -> join  (with above split)
    """

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def start(self):
        self.next(self.foreach_split_x, self.split_w)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_split_x(self):
        self.x = "ab"
        self.next(self.foreach_split_y, foreach="x")

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def split_w(self):
        self.var1 = 100
        self.next(self.w1, self.w2)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def w1(self):
        self.var1 = 150
        self.next(self.join_w)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def w2(self):
        self.var1 = 250
        self.next(self.join_w)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def join_w(self, w_inp):
        self.var1 = w_inp.w1.var1
        assert self.var1 == 150
        self.next(self.foreach_join_w_x)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_split_y(self):
        self.y = "cd"
        self.next(self.foreach_split_z, foreach="y")

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_split_z(self):
        self.z = "ef"
        self.next(self.foreach_inner, foreach="z")

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_inner(self):
        [x, y, z] = self.foreach_stack()

        # assert that lengths are correct
        assert len(self.x) == x[1]
        assert len(self.y) == y[1]
        assert len(self.z) == z[1]

        # assert that variables are correct given their indices
        assert x[2] == self.x[x[0]]
        assert y[2] == self.y[y[0]]
        assert z[2] == self.z[z[0]]

        self.combo = x[2] + y[2] + z[2]
        self.next(self.foreach_inner_2)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_inner_2(self):
        assert self.input in "ef"
        self.next(self.foreach_join_z)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_join_z(self, inputs):
        self.next(self.foreach_join_y)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_join_y(self, inputs):
        self.next(self.foreach_join_start)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_join_start(self, inputs):
        self.next(self.foreach_join_w_x)

    @resources(
        cpu="0.1",
        cpu_limit="0.5",
        memory="10M",
        memory_limit="500M"
    )
    @step
    def foreach_join_w_x(self, input):
        assert input.join_w.var1 == 150
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
    NestedForeachWithBranching()
