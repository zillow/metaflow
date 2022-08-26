from metaflow import FlowSpec, step, Parameter, JSONType
import time


class TriggerSuccessFlow(FlowSpec):
    """
    A flow for testing ability to handle JSONType objects.

    """

    # parameters w/ asserts that expect input (value should not be default at assert calls)
    str_to_list_param = Parameter(
        "str_to_list_param", type=JSONType, default="[1, 2, 3]"
    )
    str_to_dict_param = Parameter(
        "str_to_dict_param", type=JSONType, default='{"one": 1, "two": 2, "three": 3}'
    )
    list_param = Parameter("list_param", type=JSONType, default="[7, 8, 9]")
    dict_param = Parameter(
        "dict_param", type=JSONType, default='{"seven": 7, "eight": 8, "nine": 9}'
    )
    str_param = Parameter("reg_str", type=str, default="this is a string")
    date_key = Parameter("date_key", default="2022-08-25")
    int_param = Parameter("int_param", type=int, default=52)
    bool_param = Parameter("bool_param", type=bool, default=True)
    float_param = Parameter("float_param", type=float, default=543.21)

    # parameters expected to be their default values
    default_list_param = Parameter(
        "default_list_param", type=JSONType, default="['a', 'b', 'c']"
    )
    default_dict_param = Parameter(
        "default_dict_param",
        type=JSONType,
        default='{"forever": 8, "and": 8, "ever": 8}',
    )
    default_str_param = Parameter(
        "default_reg_str", type=str, default="unchanged string"
    )
    default_int_param = Parameter("default_int_param", type=int, default=1984)
    default_bool_param = Parameter("default_bool_param", type=bool, default=False)
    default_float_param = Parameter("default_float_param", type=float, default=3.14159)

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("TriggerSuccessFlow is starting.")
        time.sleep(5)  # ensures that
        self.next(self.hello)

    @step
    def hello(self):
        """
        A step to print out JSONType objects.

        """
        print("Ready to begin testing JSONType object params")
        print(
            f"str_to_list_param - type: {type(self.str_to_list_param)}, data: {self.str_to_list_param}"
        )
        print(
            f"str_to_dict_param - type: {type(self.str_to_dict_param)}, data: {self.str_to_dict_param}"
        )
        print(f"list_param - type: {type(self.list_param)}, data: {self.list_param}")
        print(f"dict_param - type: {type(self.dict_param)}, data: {self.dict_param}")

        expected_values = [
            # parameters w/ asserts that expect input
            (self.str_to_list,),
            (self.str_to_dict,),
            (self.list_param,),
            (self.dict_param,),
            (self.str_param,),
            (self.date_key,),
            (self.int_param,),
            (self.bool_param,),
            (self.float_param,),
            # parameters expected to be their default values
            (self.default_list_param, ["a", "b", "c"]),
            (self.default_dict_param, {"eight": 8, "acht": 8, "ocho": 8}),
            (self.default_str_param, "unchanged string"),
            (self.default_int_param, 1984),
            (self.default_bool_param, False),
            (self.default_float_param, 3.14159),
        ]

        for actual, expected in expected_values:
            assert (
                actual == expected
            ), f"actual value {actual} not equal to expected value {expected}"

        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step.

        """
        print("TriggerSuccessFlow is all done.")


if __name__ == "__main__":
    TriggerSuccessFlow()
