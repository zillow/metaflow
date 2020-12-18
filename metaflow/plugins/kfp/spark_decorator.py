from metaflow.decorators import StepDecorator


class SparkDecorator(StepDecorator):
    """
    This decorator is solely needed to change the base image in steps which
    execute Spark commands to one that has spark-submit installed.
    """

    name = "spark"