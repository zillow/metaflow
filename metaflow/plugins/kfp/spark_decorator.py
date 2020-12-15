from metaflow.decorators import StepDecorator


class SparkDecorator(StepDecorator):
    """
    TODO: add documentation.
    """

    name = "spark"

    defaults = {
        "conf": ["spark.eventLog.enabled=false"]
    }
