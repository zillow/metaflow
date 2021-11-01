
from typing import Dict, List

def kfp_step_function_invoker(
    init_cmd: str,
    cmd_template: str,
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
    import os, subprocess, sys
    sys.path.append(os.path.join(os.getcwd(), 'metaflow'))
    with subprocess.Popen(
        init_cmd, shell=True, universal_newlines=True, executable="/bin/bash"
    ) as process:
        pass
    from metaflow.plugins.kfp.kfp_step_function import kfp_step_function 
    
    return kfp_step_function(
        cmd_template,
        metaflow_run_id,
        metaflow_configs,
        cd_cmd,
        clean_volume_cmd,
        step_cli,
        task_id_template,
        step_name,
        flow_name,
        passed_in_split_indexes,
        preceding_component_inputs,
        preceding_component_outputs,
        flow_parameters_json,
        **kwargs
    )
