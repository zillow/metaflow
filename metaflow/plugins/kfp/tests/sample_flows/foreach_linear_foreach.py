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


class ForeachLinearForeach(FlowSpec):
    """
    foreach -> linear -> linear -> foreach -> linear -> linear -> join
    """

    @step
    def start(self):
        self.x = "ab"
        self.next(self.linear_1, foreach="x")

    @step
    def linear_1(self):
        self.next(self.linear_2)

    @step
    def linear_2(self):
        self.next(self.foreach_split_z)

    @step
    def foreach_split_z(self):
        self.z = "ef"
        self.next(self.linear_3, foreach="z")

    @step
    def linear_3(self):
        self.next(self.linear_4)

    @step
    def linear_4(self):
        self.next(self.foreach_join_z)

    @step
    def foreach_join_z(self, inputs):
        # pprint.pprint([(input.x, input.y, input.z) for input in inputs])
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachLinearForeach()
