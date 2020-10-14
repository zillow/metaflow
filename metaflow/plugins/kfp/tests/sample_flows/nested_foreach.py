# -*- coding: utf-8 -*-
from metaflow import FlowSpec, step, Parameter, JSONType
import pprint


def truncate(var):
    var = str(var)
    if len(var) > 500:
        var = "%s..." % var[:500]
    return var


class ExpectationFailed(Exception):
    def __init__(self, expected, got):
        super(ExpectationFailed, self).__init__(
            "Expected result: %s, got %s" % (truncate(expected), truncate(got))
        )


def assert_equals(expected, got):
    if expected != got:
        raise ExpectationFailed(expected, got)


class NestedForeach(FlowSpec):
    @step
    def start(self):
        self.x = "ab"
        self.next(self.foreach_split_y, foreach="x")

    @step
    def foreach_split_y(self):
        self.y = "cd"
        self.next(self.foreach_split_z, foreach="y")

    @step
    def foreach_split_z(self):
        self.z = "ef"
        self.next(self.foreach_inner, foreach="z")

    @step
    def foreach_inner(self):
        pprint.pprint(self.input)
        [x, y, z] = self.foreach_stack()

        # assert that lengths are correct
        assert_equals(len(self.x), x[1])
        assert_equals(len(self.y), y[1])
        assert_equals(len(self.z), z[1])

        # assert that variables are correct given their indices
        assert_equals(x[2], self.x[x[0]])
        assert_equals(y[2], self.y[y[0]])
        assert_equals(z[2], self.z[z[0]])

        self.combo = x[2] + y[2] + z[2]
        self.next(self.foreach_inner_2)

    @step
    def foreach_inner_2(self):
        assert self.input in "ef"
        self.next(self.foreach_join_z)

    @step
    def foreach_join_z(self, inputs):
        pprint.pprint([(input.x, input.y, input.z) for input in inputs])
        self.next(self.foreach_join_y)

    @step
    def foreach_join_y(self, inputs):
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    NestedForeach()
