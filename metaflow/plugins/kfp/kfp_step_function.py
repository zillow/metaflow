import click

from typing import List

from ... import R
from metaflow.plugins.kfp.kfp_constants import (
    STEP_ENVIRONMENT_VARIABLES,
    LOGS_DIR,
    STDOUT_PATH,
    STDERR_PATH,
    TASK_ID_ENV_NAME,
    SPLIT_INDEX_ENV_NAME,
    INPUT_PATHS_ENV_NAME,
    RETRY_COUNT,
)
from metaflow.mflog import bash_capture_logs, export_mflog_env_vars, BASH_SAVE_LOGS


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
    step_cli_string = " && ".join(cmds)
    return step_cli_string


def _command(
    cd_into_metaflow_package_cmd: str,
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

    step_cmds = []
    step_cmds.append("echo 'Task is starting.'")
    step_cmds.extend(step_cli)

    step_expr = bash_capture_logs(" && ".join(step_cmds))

    # construct an entry point that
    # 1) Clean attached volume if any
    # 2) Initializes the mflog environment (mflog_expr)
    # 3) Bootstraps a metaflow environment (cd_into_metaflow_package_cmd)
    # 4) Executes a task (step_expr)
    cmd_str = (
        f"{clean_volume_cmd} "
        f"&& mkdir -p {LOGS_DIR} && {mflog_expr} "
        f" {cd_into_metaflow_package_cmd} "
        f"&& {step_expr};"
    )

    # after the task has finished, we save its exit code (fail/success)
    # and persist the final logs. The whole entrypoint should exit
    # with the exit code (c) of the task.
    #
    # Note that if step_expr OOMs, this tail expression is never executed.
    # We lose the last logs in this scenario.
    cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
    return cmd_str


@click.command()
@click.option("--cd_into_metaflow_package_cmd")
@click.option("--clean_volume_cmd")
@click.option("--environment_type")
@click.option("--foreach_step/--not_foreach_step", default=False)
@click.option("--flow_name")
@click.option("--flow_parameters_json", required=False, default="")
@click.option("--logger_type")
@click.option("--metaflow_configs")
@click.option("--metaflow_run_id")
@click.option("--monitor_type")
@click.option("--namespace", required=False, default="")
@click.option("--need_split_index/--no-need_split_index", default=False)
@click.option("--passed_in_split_indexes")
@click.option("--preceding_component_inputs")
@click.option("--preceding_component_outputs")
@click.option("--preceding_component_outputs_dict")
@click.option("--script_name")
@click.option("--step_name")
@click.option("--tags")
@click.option("--task_id")
@click.option("--task_id_template")
@click.option("--user_code_retries", type=int)
@click.option("--workflow_name")
def kfp_step_function(
    cd_into_metaflow_package_cmd: str,
    clean_volume_cmd: str,
    environment_type: str,
    flow_name: str,
    flow_parameters_json: str, # json formatted string
    foreach_step: bool,
    logger_type: str,
    metaflow_configs: str,
    metaflow_run_id: str,
    monitor_type: str,
    namespace: str,
    need_split_index: bool,
    passed_in_split_indexes: str,  # only if is_inside_foreach
    preceding_component_inputs: List[str],  # fields to return from Flow state to KFP
    preceding_component_outputs: List[str], # fields to be pushed into Flow state from KFP
    preceding_component_outputs_dict: str, # json string
    script_name: str,
    step_name: str,
    tags: List[str],
    task_id: str,
    task_id_template: str,
    user_code_retries: int,
    workflow_name: str,
) -> List[str]:
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

    metaflow_configs = json.loads(metaflow_configs)
    tags = json.loads(tags)
    preceding_component_inputs = json.loads(preceding_component_inputs)
    preceding_component_outputs = json.loads(preceding_component_outputs)

    kwargs = {}
    for arg in preceding_component_outputs_dict.split(","):
        if arg:
            key, value = arg.split("=")
            kwargs[key] = value

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
        cd_into_metaflow_package_cmd,
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

    values, output_files = [], []
    if foreach_step:
        task_context_dict = {}
        # File written by kfp_decorator.py:task_finished
        KFP_METAFLOW_FOREACH_SPLITS_PATH = "/tmp/kfp_metaflow_foreach_splits_dict.json"
        if os.path.exists(KFP_METAFLOW_FOREACH_SPLITS_PATH):  # is a foreach step
            with open(KFP_METAFLOW_FOREACH_SPLITS_PATH, "r") as file:
                task_context_dict = json.load(file)

        # json serialize foreach_splits else, the NamedTuple gets serialized
        # as string and we get the following error:
        #   withParam value could not be parsed as a JSON list: ['0', '1']
        values.append(json.dumps(task_context_dict.get("foreach_splits", [])))
        output_files.append("/tmp/outputs/foreach_splits/data")

    # read fields to return from Flow state to KFP
    preceding_component_inputs_dict = {}
    if len(preceding_component_inputs) > 0:
        preceding_component_inputs_PATH = "/tmp/preceding_component_inputs.json"
        with open(preceding_component_inputs_PATH, "r") as file:
            preceding_component_inputs_dict = json.load(file)
            values += list(preceding_component_inputs_dict.values())

    # We replicate what is done in the KFP SDK _container_op.py,
    # see: https://github.com/kubeflow/pipelines/blob/master/sdk/python/kfp/dsl/_container_op.py
    # We write outputs to a tmp file, which KFP internally uses to produces the output
    # of the container op.
    for preceding_component_input in preceding_component_inputs:
        output_files.append(f"/tmp/outputs/{preceding_component_input}/data")

    # Write all the outputs of the kfp_step_function into the appropriate
    # output files which KFP uses to produce outputs for the container op.
    for idx, output_file in enumerate(output_files):
        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError:
            pass
        with open(output_file, "w") as f:
            f.write(str(values[idx]))


if __name__ == "__main__":
    kfp_step_function()
