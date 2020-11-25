from typing import NamedTuple

from kfp.components import func_to_container_op

from metaflow import FlowSpec, step, kfp


@func_to_container_op
def div_mod(dividend: int, divisor: int) -> NamedTuple("result", [('quotient', int), ('remainder', int)]):
    print(f"dividend={dividend}, divisor={divisor}")
    return divmod(dividend, divisor)


class DivModFlow(FlowSpec):
    """
    A Flow that decorates a Metaflow Step with a KFP component
    """

    @step
    def start(self):
        """
        kfp.kfp_component_inputs Flow state ["who"] is passed to the KFP component as arguments
        """
        self.dividend = 26
        self.divisor = 7
        self.next(self.end)

    @kfp(
        container_op_func=div_mod,
        kfp_component_inputs="dividend divisor",
        kfp_component_outputs="quotient remainder",
    )
    @step
    def end(self):
        print(f"quotient={type(self.quotient)}, remainder={type(self.remainder)}")
        print(f"quotient={self.quotient}, remainder={self.remainder}")


if __name__ == "__main__":
    DivModFlow()
