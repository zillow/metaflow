class LiveRun:
    """
    This object allows users to access information relating to the run it
    represents, including booleans for whether the run has been triggered, is
    running, and was successful, as well as for failed steps, and exceptions.

    However, this class is not meant to be instantiated; instead, each plugin
    or under layer should have its own class to represent a live run and each
    such class should inherit from this class.
    """

    def __init__(self):
        """
        Initialize a LiveRun object. This class is a parent class only and is
        not intended to be instantiated.

        """

    @classmethod
    def trigger_live_run(cls) -> None:
        raise Exception("Error occurred because either LiveRun class was"
                        "instantiated or child class did not override method")

    @property
    def flow_name(self) -> str:
        raise Exception("Error occurred because either LiveRun class was"
                        "instantiated or child class did not override method")

    @property
    def has_triggered(self) -> bool:
        raise Exception("Error occurred because either LiveRun class was"
                        "instantiated or child class did not override method")

    @property
    def is_running(self) -> bool:
        raise Exception("Error occurred because either LiveRun class was"
                        "instantiated or child class did not override method")

    @property
    def successful(self) -> bool:
        raise Exception("Error occurred because either LiveRun class was"
                        "instantiated or child class did not override method")

    @property
    def failed_steps(self) -> list:
        raise Exception("Error occurred because either LiveRun class was"
                        "instantiated or child class did not override method")

    @property
    def exceptions(self) -> dict:
        raise Exception("Error occurred because either LiveRun class was"
                        "instantiated or child class did not override method")

    def _print_status(self):
        print(f"""Current run properties:
        Has Triggered:    {self.has_triggered}
        Is Running:       {self.is_running}
        Successful:       {self.successful}
        Failed Steps:     {self.failed_steps}
        Exceptions:       {self.exceptions}
        """)
