import argparse
import inspect
import json
import os
from pathlib import Path
import sys

from typing import List, Dict

from ... import R
from metaflow.plugins.kfp.kfp_constants import STEP_ENVIRONMENT_VARIABLES, LOGS_DIR, STDOUT_PATH, STDERR_PATH, TASK_ID_ENV_NAME, SPLIT_INDEX_ENV_NAME, INPUT_PATHS_ENV_NAME, RETRY_COUNT
from metaflow.mflog import bash_capture_logs, export_mflog_env_vars, BASH_SAVE_LOGS

import metaflow

def _step_cli(
    node_name: str,
    task_id: str,
    metaflow_run_id: str,
    namespace: str,
    tags: List[str],
    need_split_index: bool,
    environment_type: str,
    logger_type: str,
    monitor_type: str,
    user_code_retries: int,
    workflow_name: str,
    script_name: str,
) -> str:
    """
    Analogous to step_functions_cli.py
    This returns the command line to run the internal Metaflow step click entrypiont.
    """
    cmds = []

    executable = "python3" if R.use_r() else "python"

    if R.use_r():
        entrypoint = [R.entrypoint()]
    else:
        entrypoint = [executable, script_name]

    start_task_id_params_path = None

    tags_extended = [
        f"--tag argo_workflow:{workflow_name}",
        "--tag pod_name:$MF_POD_NAME",
        "--tag pod_namespace:$MF_POD_NAMESPACE",
        # TODO(talebz): A Metaflow plugin framework to customize tags, labels, etc.
        "--tag zodiac_service:$ZODIAC_SERVICE",
        "--tag zodiac_team:$ZODIAC_TEAM",
    ]
    if tags:
        tags_extended.extend("--tag %s" % tag for tag in tags)

    if node_name == "start":
        # We need a separate unique ID for the special _parameters task
        task_id_params = "1-params"

        # Export user-defined parameters into runtime environment
        param_file = "parameters.sh"
        # TODO: move to KFP plugin
        export_params = (
            "python -m "
            "metaflow.plugins.aws.step_functions.set_batch_environment "
            "parameters %s && . `pwd`/%s" % (param_file, param_file)
        )
        params = entrypoint + [
            "--quiet",
            "--environment=%s" % environment_type,
            "--datastore=s3",
            "--datastore-root=$METAFLOW_DATASTORE_SYSROOT_S3",
            "--event-logger=%s" % logger_type,
            "--monitor=%s" % monitor_type,
            "--no-pylint",
            "init",
            "--run-id %s" % metaflow_run_id,
            "--task-id %s" % task_id_params,
        ]

        params.extend(tags_extended)

        # If the start step gets retried, we must be careful not to
        # regenerate multiple parameters tasks. Hence we check first if
        # _parameters exists already.
        start_task_id_params_path = (
            "{metaflow_run_id}/_parameters/{task_id_params}".format(
                metaflow_run_id=metaflow_run_id, task_id_params=task_id_params
            )
        )
        exists = entrypoint + [
            "dump",
            "--max-value-size=0",
            start_task_id_params_path,
        ]
        cmd = "if ! %s >/dev/null 2>/dev/null; then %s && %s; fi" % (
            " ".join(exists),
            export_params,
            " ".join(params),
        )
        cmds.append(cmd)

    top_level = [
        "--quiet",
        "--environment=%s" % environment_type,
        "--datastore=s3",
        "--datastore-root=$METAFLOW_DATASTORE_SYSROOT_S3",
        "--event-logger=%s" % logger_type,
        "--monitor=%s" % monitor_type,
        "--no-pylint",
    ]

    cmds.append(
        " ".join(
            entrypoint
            + top_level
            + [
                "kfp step-init",
                "--run-id %s" % metaflow_run_id,
                "--step_name %s" % node_name,
                '--passed_in_split_indexes "{passed_in_split_indexes}"',
                "--task_id %s" % task_id,  # the assigned task_id from Flow graph
            ]
        )
    )

    # load environment variables set in STEP_ENVIRONMENT_VARIABLES
    cmds.append(f". {STEP_ENVIRONMENT_VARIABLES}")

    step = [
        "--with=kfp",
        "step",
        node_name,
        "--run-id %s" % metaflow_run_id,
        f"--task-id ${TASK_ID_ENV_NAME}",
        f"--retry-count ${RETRY_COUNT}",
        "--max-user-code-retries %d" % user_code_retries,
        (
            "--input-paths %s" % start_task_id_params_path
            if node_name == "start"
            else f"--input-paths ${INPUT_PATHS_ENV_NAME}"
        ),
    ]

    if need_split_index:
        step.append(f"--split-index ${SPLIT_INDEX_ENV_NAME}")

    step.extend(tags_extended)

    if namespace:
        step.append("--namespace %s" % namespace)

    cmds.append(" ".join(entrypoint + top_level + step))
    step_cli_string =  " && ".join(cmds)
    return step_cli_string

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
        f" {cd_cmd} " 
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
    metaflow_configs: Dict[str, str],#str,#Dict[str, str],
    cd_cmd: str,
    clean_volume_cmd: str,
    task_id: str,
    task_id_template: str,
    step_name: str,
    flow_name: str,
    namespace: str,
    tags: List[str],#str,#List[str],
    need_split_index: bool,
    environment_type: str,
    logger_type: str,
    monitor_type: str,
    user_code_retries: int,
    workflow_name: str,
    script_name: str,
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

    step_cli = _step_cli(
        step_name,
        task_id,
        metaflow_run_id,
        namespace,
        tags,
        need_split_index,
        environment_type,
        logger_type,
        monitor_type,
        user_code_retries,
        workflow_name,
        script_name,
    )

    # expose passed KFP passed in arguments as environment variables to
    # the bash command
    preceding_component_outputs_env: Dict[str, str] = {
        field: kwargs[field] for field in preceding_component_outputs
    }
    cmd_template = _command(
        cd_cmd,
        clean_volume_cmd,
        [step_cli],
        task_id_template,
        step_name,
        flow_name,
    )
    cmd = cmd_template.format(
        run_id=metaflow_run_id,
        passed_in_split_indexes=passed_in_split_indexes,
    )

    print("passed_in_split_indexes: ", passed_in_split_indexes)
    print("cmd: ", cmd)

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
        print(flow_parameters_json, type(flow_parameters_json))
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
    print("ret: ", ret)
    return ret

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--metaflow_run_id", type=str, required=True)
    parser.add_argument("--metaflow_configs", type=json.loads, required=True)
    parser.add_argument("--cd_cmd", type=str, required=True)
    parser.add_argument("--clean_volume_cmd", type=str, required=True)
    parser.add_argument("--task_id", type=str, required=True)
    parser.add_argument("--task_id_template", type=str, required=True)
    parser.add_argument("--step_name", type=str, required=True)
    parser.add_argument("--flow_name", type=str, required=True)
    parser.add_argument("--namespace", action='store_true')
    parser.add_argument("--tags", type=json.loads, required=True)
    parser.add_argument("--need_split_index", action='store_true')
    parser.add_argument("--environment_type", type=str, required=True)
    parser.add_argument("--logger_type", type=str, required=True)
    parser.add_argument("--monitor_type", type=str, required=True)
    parser.add_argument("--user_code_retries", type=int, required=True)
    parser.add_argument("--workflow_name", type=str, required=True)
    parser.add_argument("--script_name", type=str, required=True)
    parser.add_argument("--passed_in_split_indexes", type=str, required=False)
    parser.add_argument("--preceding_component_inputs", type=json.loads, required=False)
    parser.add_argument("--preceding_component_outputs", type=json.loads, required=False)
    parser.add_argument("--flow_parameters_json", type=str, required=False)
    args = parser.parse_args()

    _parsed_args = vars(args)
    #_output_files = _parsed_args.pop("_output_paths", [])
    _output_files = ["/tmp/outputs/foreach_splits/data"]
    _outputs = kfp_step_function(
        args.metaflow_run_id,
        args.metaflow_configs,
        args.cd_cmd,
        args.clean_volume_cmd,
        args.task_id,
        args.task_id_template,
        args.step_name,
        args.flow_name,
        args.namespace,
        args.tags,
        args.need_split_index,
        args.environment_type,
        args.logger_type,
        args.monitor_type,
        args.user_code_retries,
        args.workflow_name,
        args.script_name,
        args.passed_in_split_indexes,
        flow_parameters_json=args.flow_parameters_json,
    )

    _output_serializers = [str]
    import os
    for idx, output_file in enumerate(_output_files):
        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError:
            pass
        with open(output_file, 'w') as f:
            f.write(_output_serializers[idx](_outputs[idx]))
