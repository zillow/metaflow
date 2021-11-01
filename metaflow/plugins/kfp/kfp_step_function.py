import inspect
import os
from pathlib import Path

from typing import List, Dict

from metaflow.plugins.kfp.kfp_constants import STEP_ENVIRONMENT_VARIABLES, LOGS_DIR, STDOUT_PATH, STDERR_PATH
from metaflow.mflog import bash_capture_logs, export_mflog_env_vars, BASH_SAVE_LOGS

def _command(
    cd_cmd: str,
    clean_volume_cmd: str,
    step_cli: List[str],
    task_id_template: str,
    step_name: str,
    flow_name: str,
) -> str:
    """
    Analogous to batch.py
    """
    retry_count_python = (
        "import os;"
        'name = os.environ.get("MF_ARGO_NODE_NAME");'
        'index = name.rfind("(");'
        'retry_count = (0 if index == -1 else name[index + 1: -1]) if name.endswith(")") else 0;'
        "print(str(retry_count))"
    )

    mflog_expr = export_mflog_env_vars(
        flow_name=flow_name,
        run_id="{run_id}",
        step_name=step_name,
        task_id=task_id_template,
        retry_count=f"`python -c '{retry_count_python}'`",
        datastore_type="s3",
        datastore_root="$METAFLOW_DATASTORE_SYSROOT_S3",
        stdout_path=STDOUT_PATH,
        stderr_path=STDERR_PATH,
    )

    # if self.s3_code_package:
    #     cd_cmd = "cd metaflow"
    # else:
    #     cd_cmd = (
    #         "cd " + str(Path(inspect.getabsfile(self.flow.__class__)).parent)
    #     )


    step_cmds = []
    # step_cmds.extend(environment.bootstrap_commands(step_name))
    step_cmds.append("echo 'Task is starting.'")
    step_cmds.extend(step_cli)

    step_expr = bash_capture_logs(" && ".join(step_cmds))

    # if "volume" in resource_requirements:
    #     volume_dir = resource_requirements["volume_dir"]
    #     clean_volume = f"rm -rf {os.path.join(volume_dir, '*')}"
    # else:
    #     # the `true` command is to make sure that the generated command
    #     # plays well with docker containers which have entrypoint set as
    #     # eval $@
    #     clean_volume = "true"

    # construct an entry point that
    # 1) Clean attached volume if any
    # 2) Initializes the mflog environment (mflog_expr)
    # 3) Bootstraps a metaflow environment (init_expr)
    # 4) Executes a task (step_expr)
    cmd_str = (
        f"{clean_volume_cmd} "
        f"&& mkdir -p {LOGS_DIR} && {mflog_expr} "
        f"&& {cd_cmd} "
        f"&& {step_expr};"
    )

    # after the task has finished, we save its exit code (fail/success)
    # and persist the final logs. The whole entrypoint should exit
    # with the exit code (c) of the task.
    #
    # Note that if step_expr OOMs, this tail expression is never executed.
    # We lose the last logs in this scenario.
    cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
    # print("cmd_str: ", cmd_str)
    return cmd_str

def kfp_step_function(
    metaflow_run_id: str,
    metaflow_configs: Dict[str, str],
    cd_cmd: str,
    clean_volume_cmd: str,
    step_cli: List[str],
    task_id_template: str,
    step_name: str,
    flow_name: str,
    passed_in_split_indexes: str = "",  # only if is_inside_foreach
    preceding_component_inputs: List[
        str
    ] = None,  # fields to return from Flow state to KFP
    preceding_component_outputs: List[
        str
    ] = None,  # fields to be pushed into Flow state from KFP
    flow_parameters_json: str = None,  # json formatted string
    **kwargs,
) -> object:
    """
    Renders and runs the cmd_template containing Metaflow step-init commands to
    run within the container.

    Returns: namedtuple(["foreach_splits"] + preceding_component_inputs)
    """
    import os
    import json
    import logging
    from subprocess import Popen
    from collections import namedtuple
    from typing import Dict

    if preceding_component_inputs is None:
        preceding_component_inputs = []
    if preceding_component_outputs is None:
        preceding_component_outputs = []

    # expose passed KFP passed in arguments as environment variables to
    # the bash command
    preceding_component_outputs_env: Dict[str, str] = {
        field: kwargs[field] for field in preceding_component_outputs
    }
    cmd_template = _command(
        cd_cmd,
        clean_volume_cmd,
        step_cli,
        task_id_template,
        step_name,
        flow_name,
    )
    cmd = cmd_template.format(
        run_id=metaflow_run_id,
        passed_in_split_indexes=passed_in_split_indexes,
    )

    metaflow_configs_new = {
        name: value for name, value in metaflow_configs.items() if value
    }

    if (
        not "METAFLOW_USER" in metaflow_configs_new
        or metaflow_configs_new["METAFLOW_USER"] is None
    ):
        metaflow_configs_new["METAFLOW_USER"] = "kfp-user"

    env = {
        **os.environ,
        **metaflow_configs_new,
        "PRECEDING_COMPONENT_INPUTS": json.dumps(preceding_component_inputs),
        "PRECEDING_COMPONENT_OUTPUTS": json.dumps(preceding_component_outputs),
        **preceding_component_outputs_env,
    }
    if flow_parameters_json is not None:
        env["METAFLOW_PARAMETERS"] = flow_parameters_json
    
    # TODO: Map username to KFP specific user/profile/namespace
    # Running Metaflow
    # KFP orchestrator -> running MF runtime (runs user code, handles state)
    with Popen(
        cmd, shell=True, universal_newlines=True, executable="/bin/bash", env=env
    ) as process:
        pass

    if process.returncode != 0:
        logging.info(f"---- Following command returned: {process.returncode}")
        logging.info(cmd.replace(" && ", "\n"))
        logging.info("----")
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
    preceding_component_inputs_dict = {}
    if len(preceding_component_inputs) > 0:
        preceding_component_inputs_PATH = "/tmp/preceding_component_inputs.json"
        with open(preceding_component_inputs_PATH, "r") as file:
            preceding_component_inputs_dict = json.load(file)
            values += list(preceding_component_inputs_dict.values())

    ret = namedtuple(
        "StepOpRet", ["foreach_splits"] + list(preceding_component_inputs_dict.keys())
    )(*values)
    return ret
