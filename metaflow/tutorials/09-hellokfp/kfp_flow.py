from kfp.components import func_to_container_op

from metaflow import FlowSpec, step, kfp
from collections import namedtuple


@func_to_container_op
def my_step_op_func(x: str, y: str) -> namedtuple("foo", "r1 r2"):
    """
    returns r1_value and r2_value appended to x and y respectively
    """
    from collections import namedtuple

    print(f"x input parameter: {x}")
    print(f"y input parameter: {y}")

    assert x == "x_value"
    assert y == "y_value"

    return namedtuple("ret", "r1 r2")(*[x + " r1_value", y + " r2_value"])


class KfpFlow(FlowSpec):
    """
    A Flow that decorates a Metaflow Step with a KFP component
    """

    @kfp(
        func=my_step_op_func,
        kfp_component_inputs=["x", "y"],
        kfp_component_outputs=["r1", "r2"],
    )
    @step
    def start(self):
        """
        kfp.kfp_component_inputs Flow state ["x", "y"] is passed to the KFP component as arguments
        """
        self.x = "x_value"
        self.y = "y_value"

        self.next(self.end)

    @step
    def end(self):
        """
        kfp.kfp_component_outputs ["r1", "r2'] are now available as Metaflow Flow state
        """
        print("r1", self.r1)
        print("r2", self.r2)

        assert self.r1 == "x_value r1_value"
        assert self.r2 == "y_value r2_value"


if __name__ == "__main__":
    KfpFlow()
