from metaflow import FlowSpec, step, Parameter, JSONType


class HelloJSONType(FlowSpec):
    """
    A flow where Metaflow prints 'Hi'.

    Run this flow to validate that Metaflow is installed correctly.

    """

    # a user-made class for testing
    class ChrisVan:

        def __init__(self):
            self.hello = "Hi, this is Chris Van, but you can call me Chris"

        def say_hi(self):
            print(self.hello)

    str_to_list = Parameter("str_to_list",
                            type=JSONType,
                            default="[1, 2, 3]")
    str_to_dict = Parameter("str_to_dict",
                            type=JSONType,
                            default="{'one': 1, 'two': 2, 'three': 3}")
    str_to_set = Parameter("str_to_set",
                            type=JSONType,
                            default="{1, 2, 3}")
    str_to_ = Parameter("str_to_dict",
                            type=JSONType,
                            default="{'one': 1, 'two': 2, 'three': 3}")


    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.

        """
        print("HelloFlow is starting.")
        self.next(self.hello)

    @step
    def hello(self):
        """
        A step for metaflow to introduce itself.

        """
        print("Metaflow says: Hi from the inner loop?!")
        print(self.date_key)
        
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.

        """
        print("HelloFlow is all done.")


if __name__ == "__main__":
    HelloJSONType()
