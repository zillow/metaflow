
from typing import Dict, List

def kfp_step_function_invoker(
    init_cmd: str,
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
    import os, subprocess, sys
    sys.path.append(os.path.join(os.getcwd(), 'metaflow'))
    print(init_cmd)
    # print(sys.path)
    # print(code_package_template)
    # init_cmd = "mkdir -p /opt/metaflow_volume/metaflow_logs && export MFLOG_STDOUT=/opt/metaflow_volume/metaflow_logs/mflog_stdout && " + init_cmd
    with subprocess.Popen(
        init_cmd, shell=True, universal_newlines=True, executable="/bin/bash"
    ) as process:
        pass
    # print(os.listdir("./metaflow"))
    # print(os.getcwd())
    # os.chdir("./metaflow")
    # print(os.listdir("./"))
    # print("stdout: ", process.stdout)
    # print("stderr: ", process.stderr)
    # from metaflow.plugins.kfp.kfp_step_function import kfp_step_function
    # kfp_step_function = "python -m metaflow.plugins.kfp.kfp_step_function"
    # with Popen(
    #     kfp_step_function, shell=True, universal_newlines=True, executable="/bin/bash"
    # ) as process:
    #     pass
    from metaflow.plugins.kfp.kfp_step_function import kfp_step_function
    # os.chdir("../")
    
    return kfp_step_function(
        cmd_template,
        metaflow_run_id,
        metaflow_configs,
        passed_in_split_indexes,
        preceding_component_inputs,
        preceding_component_outputs,
        flow_parameters_json,
        **kwargs
    )
