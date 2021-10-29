
from typing import Dict, List

def kfp_step_function_invoker(
    code_package_template: str,
    cmd_template: str,
    metaflow_run_id: str,
    metaflow_configs: Dict[str, str],
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
    from subprocess import Popen
    print(code_package_template)
    code_package_template = "export MFLOG_STDOUT=/opt/metaflow_volume/metaflow_logs/mflog_stdout && " + code_package_template
    with Popen(
        code_package_template, shell=True, universal_newlines=True, executable="/bin/bash"
    ) as process:
        pass
    import os
    print(os.listdir("./"))
    print("stdout: ", process.stdout)
    print("stderr: ", process.stderr)
    from metaflow.plugins.kfp.kfp_step_function import kfp_step_function
    print("Called here!")
    return kfp_step_function(
        cmd_template,
        metaflow_run_id,
        metaflow_configs,
        passed_in_split_indexes,
        preceding_component_inputs,
        preceding_component_outputs,
        flow_parameters_json,
        kwargs
    )
