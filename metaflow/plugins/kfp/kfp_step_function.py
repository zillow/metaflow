def kfp_step_function(**kwargs) -> object:
    """
    (
      datastore_root: str,
      cmd_template: str,
      kfp_run_id: str,
      passed_in_split_indexes: str = '""',  # only if is_inside_foreach
    ) -> namedtuple(["foreach_splits"] + kfp_component_inputs)

    Renders and runs the cmd_template containing Metaflow step/init commands to
    run within the container.
    """
    import os
    import json
    from subprocess import Popen
    from collections import namedtuple
    from metaflow.cli import logger
    from typing import Dict, List

    datastore_root = kwargs["datastore_root"]
    cmd_template = kwargs["cmd_template"]
    kfp_run_id = kwargs["kfp_run_id"]
    passed_in_split_indexes = kwargs["passed_in_split_indexes"]
    # fields to return from Flow state to KFP
    kfp_component_inputs: List[str] = json.loads(
        kwargs.get("kfp_component_inputs", "[]")
    )
    # fields to be pushed into Flow state from KFP
    kfp_component_outputs: List[str] = json.loads(
        kwargs.get("kfp_component_outputs", "[]")
    )
    # expose passed KFP passed in arguments as environment variables to
    # the bash command
    kfp_component_outputs_env: Dict[str, str] = {
        field: kwargs[field] for field in kfp_component_outputs
    }
    metaflow_service_url: str = kwargs.get("metaflow_service_url", "")

    cmd = cmd_template.format(
        run_id=kfp_run_id,
        datastore_root=datastore_root,
        passed_in_split_indexes=passed_in_split_indexes,
    )

    # TODO: Map username to KFP specific user/profile/namespace
    # Running Metaflow
    # KFP orchestrator -> running MF runtime (runs user code, handles state)
    with Popen(
        cmd,
        shell=True,
        universal_newlines=True,
        executable="/bin/bash",
        env={
            **dict(
                os.environ,
                METAFLOW_DATASTORE_SYSROOT_S3=datastore_root,
                KFP_COMPONENT_INPUTS=json.dumps(kfp_component_inputs),
                KFP_COMPONENT_OUTPUTS=json.dumps(kfp_component_outputs),
                METAFLOW_USER="kfp-user",  # TODO: what should this be for a non-scheduled run?
                METAFLOW_SERVICE_URL=metaflow_service_url,
            ),
            **kfp_component_outputs_env,
        },
    ) as process:
        pass

    if process.returncode != 0:
        logger(f"---- Following command returned: {process.returncode}")
        logger(cmd.replace(" && ", "\n"))
        logger("----")
        raise Exception("Returned: %s" % process.returncode)

    task_context_dict = {}
    # File written by kfp_decorator.py:task_finished
    KFP_METAFLOW_FOREACH_SPLITS_PATH = "/tmp/kfp_metaflow_foreach_splits_dict.json"
    if os.path.exists(KFP_METAFLOW_FOREACH_SPLITS_PATH):  # is a foreach step
        with open(KFP_METAFLOW_FOREACH_SPLITS_PATH, "r") as file:
            task_context_dict = json.load(file)

    # json serialize foreach_splits else, the NamedTuple gets serialized
    # as string and we get the following error:
    #   withParam value could not be parsed as a JSON list: ['0', '1']
    values = [json.dumps(task_context_dict.get("foreach_splits", []))]

    # read fields to return from Flow state to KFP
    kfp_component_inputs_dict = {}
    if len(kfp_component_inputs) > 0:
        KFP_COMPONENT_INPUTS_PATH = "/tmp/kfp_component_inputs.json"
        with open(KFP_COMPONENT_INPUTS_PATH, "r") as file:
            kfp_component_inputs_dict = json.load(file)
        values += list(kfp_component_inputs_dict.values())

    ret = namedtuple(
        "StepOpRet", ["foreach_splits"] + list(kfp_component_inputs_dict.keys())
    )(*values)
    print("ret", ret)
    return ret
