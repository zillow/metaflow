from metaflow.decorators import StepDecorator


class PodAnnotationDecorator(StepDecorator):
    """
    Add a single pod annotation. This decorator can be used multiple time per step.
    Repeatedly assigning value under same key will overwrite previous value.
    """

    name = "pod_annotation"
    defaults = {
        "key": None,
        "value": None,
    }


class PodLabelDecorator(StepDecorator):
    """
    Add a single pod label. This decorator can be used multiple time per step.
    Repeatedly assigning value under same key will overwrite previous value.
    """

    name = "pod_label"
    defaults = {
        "key": None,
        "value": None,
    }
