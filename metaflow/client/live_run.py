from __future__ import annotations


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
        self._flow_name = None
        self._metaflow_run_id = None

    @classmethod
    def trigger(cls, **kwargs) -> LiveRun:
        raise NotImplementedError(
            "Error occurred because either LiveRun class was"
            "instantiated or child class did not override method"
        )

    @property
    def flow_name(self) -> str:
        """
        Returns Metaflow flow name.

        NOTE: The method will never actually return None because instances of
        child classes should only be created using the classmethod 'trigger',
        and trigger populates flow_name with the correct info before returning.
        """
        return self._flow_name

    @property
    def run_id(self) -> str:
        """
        Returns Metaflow run id.

        NOTE: The method will never actually return None because instances of
        child classes should only be created using the classmethod 'trigger',
        and trigger populates run_id with the correct info before returning.
        """
        return self._metaflow_run_id

    @property
    def has_triggered(self) -> bool:
        raise NotImplementedError(
            "Error occurred because either LiveRun class was"
            "instantiated or child class did not override method"
        )

    @property
    def is_running(self) -> bool:
        raise NotImplementedError(
            "Error occurred because either LiveRun class was"
            "instantiated or child class did not override method"
        )

    @property
    def successful(self) -> bool:
        raise NotImplementedError(
            "Error occurred because either LiveRun class was"
            "instantiated or child class did not override method"
        )

    @property
    def failed_steps(self) -> list:
        raise NotImplementedError(
            "Error occurred because either LiveRun class was"
            "instantiated or child class did not override method"
        )

    @property
    def exceptions(self) -> dict:
        raise NotImplementedError(
            "Error occurred because either LiveRun class was"
            "instantiated or child class did not override method"
        )

    def _print_status(self):
        print(
            f"""Current run properties:
        Has Triggered:    {self.has_triggered}
        Is Running:       {self.is_running}
        Successful:       {self.successful}
        Failed Steps:     {self.failed_steps}
        Exceptions:       {self.exceptions}
        """
        )
