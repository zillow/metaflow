from metaflow import FlowSpec, step, Parameter, JSONType
import time


class LiveRunParamFlow(FlowSpec):
    """
    A flow for testing ability to handle parameters of different types,
    including str, list, dict, int, bool, and float. Also tests ability to take
    in parameters or use default parameters.
    """

    # parameters w/ asserts that expect input (value should not be default)
    str_to_list = Parameter("str_to_list", type=JSONType, default="[1, 2, 3]")
    str_to_dict = Parameter("str_to_dict", type=JSONType, default='{"o": 1, "w": 2}')
    list_param = Parameter("list_param", type=JSONType, default="[7, 8, 9]")
    dict_param = Parameter("dict_param", type=JSONType, default='{"e": 8, "n": 9}')
    str_param = Parameter("str_param", type=str, default="this is a string")
    date_key = Parameter("date_key", default="2022-08-25")
    int_param = Parameter("int_param", type=int, default=52)
    bool_param = Parameter("bool_param", type=bool, default=True)
    float_param = Parameter("float_param", type=float, default=543.21)

    # parameters expected to be their default values
    default_list = Parameter("default_list", type=JSONType, default='["a", "b"]')
    default_dict = Parameter(
        "default_dict", type=JSONType, default='{"for": 4, "ate": 8}'
    )
    default_str = Parameter("default_str", type=str, default="unchanged str")
    default_int = Parameter("default_int", type=int, default=1984)
    default_bool = Parameter("default_bool", type=bool, default=False)
    default_float = Parameter("default_float", type=float, default=3.14159)

    @step
    def start(self):
        """
        This step tests whether the flow has the correct parameters, including
        passed in and default values.
        """
        print("LiveRunParamFlow is starting")
        expected_values = [
            # parameters w/ asserts that expect input
            (self.str_to_list, [100, 200, 300]),
            (self.str_to_dict, {"a": 5, "b": 10}),
            (self.list_param, [33, 44, "r"]),
            (self.dict_param, {"y": 5, "z": 10}),
            (self.str_param, "NEW string"),
            (self.date_key, "1988-10-31"),
            (self.int_param, 999),
            (self.bool_param, False),
            (self.float_param, 789.654),
            # parameters expected to be their default values
            (self.default_list, ["a", "b"]),
            (self.default_dict, {"for": 4, "ate": 8}),
            (self.default_str, "unchanged str"),
            (self.default_int, 1984),
            (self.default_bool, False),
            (self.default_float, 3.14159),
        ]

        for actual, expected in expected_values:
            if expected in [False, True, None]:
                assert (
                    actual is expected
                ), f"actual '{actual}' is not expected '{expected}'"
            else:
                assert actual == expected, f"actual '{actual}' != expected '{expected}'"

        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step.
        """
        print("LiveRunParamFlow is all done.")


if __name__ == "__main__":
    LiveRunParamFlow()
