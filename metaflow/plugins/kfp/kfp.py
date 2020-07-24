import kfp
from kfp import dsl

from .constants import DEFAULT_RUN_NAME, DEFAULT_EXPERIMENT_NAME, DEFAULT_FLOW_CODE_URL, DEFAULT_KFP_YAML_OUTPUT_PATH
from typing import NamedTuple

def get_ordered_steps(graph):
    """
    Returns the ordered step names in the graph (FlowGraph) from start step to end step as a list of strings containing the
    step names.

    # TODO: Support other Metaflow graphs, as branching and joins are not currently supported
    Note: All MF graphs start at the "start" step and end with the "end" step (both of which are mandatory).
    """

    ordered_steps = ['start']
    current_node_name = 'start'

    # This is not an ideal way to iterate over the graph, but it's the (easy+)right thing to do for now.
    # This may need to change as work on improvements.
    while current_node_name != 'end':
        for node in graph:
            if node.name == current_node_name:
                if current_node_name != 'end':
                    current_node_name = node.out_funcs[0]
                    ordered_steps.append(current_node_name)
                    break

    return ordered_steps


# def step_container_op(step_name, code_url=DEFAULT_FLOW_CODE_URL):
#     """
#     Method to create a kfp container op that corresponds to a single step in the flow.
#
#     TODO: This does not maintain state. The custom pre-start command used below would be removed once we have state accessible across KFP steps.
#     TODO: The public docker is a copy of the internal docker image we were using (borrowed from aip-kfp-example). Check if any stage here may need to be further modified later.
#     """
#
#     python_cmd = """ "python helloworld.py --datastore-root ", $1, " step {} --run-id ", $2, " --task-id ", $4, " --input-paths", $2"/"$5"/"$6 """.format(
#         step_name)
#     command_inside_awk = """ {{ print {0} }}""".format(python_cmd)
#     final_run_cmd = """ python helloworld.py pre-start | awk 'END{}' | sh """.format(command_inside_awk)
#
#     return dsl.ContainerOp(
#
#         name='StepRunner-{}'.format(step_name),
#         image='ssreejith3/mf_on_kfp:python-curl-git',
#         command= ['sh', '-c'],
#         arguments=[
#             'curl -o helloworld.py {}' \
#             ' && pip install git+https://github.com/zillow/metaflow.git@c722fceffa3011ecab68ce319cff98107cc49532' \
#             ' && export USERNAME="kfp-user" ' \
#             ' && {}'.format(code_url, final_run_cmd)
#             ])

def step_op_func(step_name: str, code_url: str, ds_root: str, run_id: str, task_id: str, prev_step_name: str, prev_task_id: str) -> NamedTuple('StepOutput', [('ds_root', str), ('run_id', str), ('next_step', str), ('next_task_id', str), ('current_step', str), ('current_task_id', str)]):
    import subprocess
    from collections import namedtuple

    subprocess.call(["curl -o helloworld.py {}".format(code_url)], shell=True)
    subprocess.call(["pip3 install kfp"], shell=True) # Using this to overcome the "module not found error when it encounters the kfp imports in code
    subprocess.call(["pip3 install --user --upgrade git+https://github.com/zillow/metaflow.git@s3-integ"], # c722fceffa3011ecab68ce319cff98107cc49532 is the commit that works well; TODO: Debug why later commits are erroring out
                    shell=True)
    subprocess.call(['export USERNAME="kfp-user"'], shell=True)

    python_cmd = "python helloworld.py --datastore-root {0} step {1} --run-id {2} --task-id {3} --input-paths {2}/{4}/{5}".format(
        ds_root, step_name, run_id, task_id, prev_step_name, prev_task_id)
    final_run_cmd = 'export USERNAME="kfp-user" && {}'.format(python_cmd)

    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Note: capture_output only works in python 3.7+
    proc_output = proc.stdout.decode('ascii')
    proc_error = proc.stderr.decode('ascii')
    print("Printing process output...")

    print(proc_output)
    print(proc_error)

    outputs = proc_output.split()
    step_output = namedtuple('StepOutput', ['ds', 'run_id', 'next_step', 'next_task_id', 'current_step', 'current_task_id'])
    print(step_output(outputs[0], outputs[1], outputs[2], outputs[3], outputs[4], outputs[5]))

    return step_output(outputs[0], outputs[1], outputs[2], outputs[3], outputs[4], outputs[5])

def pre_start_op_func(code_url)  -> NamedTuple('StepOutput', [('ds_root', str), ('run_id', str), ('next_step', str), ('next_task_id', str), ('current_step', str), ('current_task_id', str)]):
    """
    Function to invoke the pre-start step of metaflow
    """

    import subprocess
    from collections import namedtuple

    subprocess.call(["curl -o helloworld.py {}".format(code_url)], shell=True)
    subprocess.call(["pip3 install kfp"], shell=True) # Using this to overcome the "module not found error when it encounters the kfp imports in code
    subprocess.call(["pip3 install --user --upgrade git+https://github.com/zillow/metaflow.git@s3-integ"], # c722fceffa3011ecab68ce319cff98107cc49532 is the commit that works well; TODO: Debug why later commits are erroring out
                    shell=True)
    subprocess.call(['export USERNAME="kfp-user"'], shell=True)
    final_run_cmd = 'export USERNAME="kfp-user" && export METAFLOW_DATASTORE_SYSROOT_S3="s3://aip-metaflow-trials" && export METAFLOW_AWS_ARN="arn:aws:iam::170606514770:role/dev-zestimate-role" python helloworld.py --datastore="s3" --datastore-root="s3://aip-metaflow-trials" pre-start'

    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) # Note: capture_output only works in python 3.7+
    proc_output = proc.stdout.decode('ascii')
    proc_error = proc.stderr.decode('ascii')
    print("Printing proc output...")

    print(proc_output)
    print(proc_error)

    outputs = proc_output.split()
    step_output = namedtuple('StepOutput', ['ds_root', 'run_id', 'next_step', 'next_task_id', 'current_step', 'current_task_id'])
    print(step_output(outputs[0], outputs[1], outputs[2], outputs[3], outputs[4], outputs[5]))

    return step_output(outputs[0], outputs[1], outputs[2], outputs[3], outputs[4], outputs[5])

def step_container_op():
    step_op =  kfp.components.func_to_container_op(step_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return step_op

def pre_start_container_op():
    pre_start_op = kfp.components.func_to_container_op(pre_start_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return pre_start_op

def create_flow_pipeline(ordered_steps, flow_code_url=DEFAULT_FLOW_CODE_URL):
    """
    Function that creates the KFP flow pipeline and returns the path to the YAML file containing the pipeline specification.
    """

    steps = ordered_steps
    code_url = flow_code_url
    print("\nCreating the pipeline definition needed to run the flow on KFP...\n")
    print("\nCode URL of the flow to be converted to KFP: {0}\n".format(flow_code_url))

    @dsl.pipeline(
        name='MF on KFP Pipeline',
        description='Pipeline defining KFP equivalent of the Metaflow flow'
    )
    def kfp_pipeline_from_flow():
        """
        This function converts the flow steps to kfp container op equivalents
        by invoking `step_container_op` for every step in the flow and handling the order of steps.
        """

        # Store the list of steps in reverse order
        # step_container_ops = [step_container_op(step, code_url) for step in reversed(steps)]
        step_container_ops = []
        pre_start_op = (pre_start_container_op())(code_url)
        step_container_ops.append(pre_start_op)

        for step in steps:
            prev_step_outputs = step_container_ops[-1].outputs
            step_container_ops.append(
                (step_container_op())(step, code_url,
                      prev_step_outputs['ds_root'],
                      prev_step_outputs['run_id'],
                      prev_step_outputs['next_task_id'],
                      prev_step_outputs['current_step'],
                      prev_step_outputs['current_task_id']
                      )
                )
            step_container_ops[-1].after(step_container_ops[-2])

        # step_container_ops = [(step_container_op())(step, code_url) for step in reversed(steps)]

        # Each step in the list can only be executed after the next step in the list, i.e., list[-1] is executed first, followed
        # by list[-2] and so on.
        # for i in range(len(steps) - 1):
        #     step_container_ops[i].after(step_container_ops[i + 1])


    return kfp_pipeline_from_flow


# def create_flow_pipeline(ordered_steps, flow_code_url=DEFAULT_FLOW_CODE_URL):
#     """
#     Function that creates the KFP flow pipeline and returns the path to the YAML file containing the pipeline specification.
#     """
#
#     steps = ordered_steps
#     code_url = flow_code_url
#     print("\nCreating the pipeline definition needed to run the flow on KFP...\n")
#     print("\nCode URL of the flow to be converted to KFP: {0}\n", flow_code_url)
#
#     @dsl.pipeline(
#         name='MF on KFP Pipeline',
#         description='Pipeline defining KFP equivalent of the Metaflow flow'
#     )
#     def kfp_pipeline_from_flow():
#         """
#         This function converts the flow steps to kfp container op equivalents
#         by invoking `step_container_op` for every step in the flow and handling the order of steps.
#         """
#
#         # Store the list of steps in reverse order
#         step_container_ops = [step_container_op(step, code_url) for step in reversed(steps)]
#
#         # Each step in the list can only be executed after the next step in the list, i.e., list[-1] is executed first, followed
#         # by list[-2] and so on.
#         for i in range(len(steps) - 1):
#             step_container_ops[i].after(step_container_ops[i + 1])
#
#     return kfp_pipeline_from_flow


def create_run_on_kfp(flowgraph, code_url, experiment_name, run_name):
    """
    Creates a new run on KFP using the `kfp.Client()`. Note: Intermediate pipeline YAML is not generated as this creates
    the run directly using the pipeline function returned by `create_flow_pipeline`

    """

    pipeline_func = create_flow_pipeline(get_ordered_steps(flowgraph), code_url)
    run_pipeline_result = kfp.Client().create_run_from_pipeline_func(pipeline_func,
                                                                     arguments={},
                                                                     experiment_name=experiment_name,
                                                                     run_name=run_name)
    return run_pipeline_result


def create_kfp_pipeline_yaml(flowgraph, code_url, pipeline_file_path=DEFAULT_KFP_YAML_OUTPUT_PATH):
    """
    Creates a new KFP pipeline YAML using `kfp.compiler.Compiler()`. Note: Intermediate pipeline YAML is saved
    at `pipeline_file_path`

    """
    pipeline_func = create_flow_pipeline(get_ordered_steps(flowgraph), code_url)

    kfp.compiler.Compiler().compile(pipeline_func, pipeline_file_path)
    return pipeline_file_path
