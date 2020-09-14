import kfp
from kfp import dsl
from kubernetes.client.models import V1EnvVar

import functools

from .constants import DEFAULT_FLOW_CODE_URL, DEFAULT_KFP_YAML_OUTPUT_PATH, DEFAULT_DOWNLOADED_FLOW_FILENAME
from metaflow.metaflow_config import METAFLOW_AWS_ARN, METAFLOW_AWS_S3_REGION, DATASTORE_SYSROOT_S3

from collections import deque, namedtuple
from typing import NamedTuple, Iterable, List

def addition_op_func(list_of_nums: list) -> int:
    list_of_nums_int = [int(num) for num in list_of_nums] # ensure we work with integers only 
    return sum(list_of_nums_int)

def foreach_op_func(python_cmd_template, step_name: str,
                 code_url: str,
                 kfp_run_id: str,
                 task_id: int,
                 parent_task_ids: list = None,
                 parent_step_names: list = None) -> NamedTuple('output', [('iterable', Iterable), ('length', int), ('task_id', int)]):
    """
    Function used to create a KFP container op (see: `step_container_op`) that corresponds to a single step in the flow.
    """
    import subprocess
    import os
    from collections import namedtuple

    def execute(cmd):
        """
        Helper function to run the given command and print `stderr` and `stdout`.
        """
        proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        proc_output = proc.stdout
        proc_error = proc.stderr

        if len(proc_error) > 1:
            print("_____________ STDERR:_____________________________")
            print(proc_error)

        if len(proc_output) > 1:
            print("______________ STDOUT:____________________________")
            print(proc_output)
    
    def parse_stdout_for_numsplits(stdout: str) -> int:
        numeral_string = "" # holds the value of the numeral num_splits
        flag = False
        for idx, char in enumerate(stdout):
            if flag:
                if char == '\n':
                    return int(numeral_string)
                else:
                    numeral_string = numeral_string + char # could be multiple digits long
            if char == ':' and idx >= 10:
                if stdout[idx - 10:idx + 1] == "num_splits:":
                    flag = True
        return -1 # -1 indicates num_splits couldn't be found

    MODIFIED_METAFLOW_URL = 'git+https://github.com/zillow/metaflow.git@feature/for_each_flow'
    DEFAULT_DOWNLOADED_FLOW_FILENAME = 'downloaded_flow.py'

    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    subprocess.call(
        ["curl -o {downloaded_file_name} {code_url}".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            code_url=code_url)], shell=True)

    print("\n----------RUNNING: KFP Installation---------------")
    subprocess.call(["pip3 install kfp"], shell=True)  # TODO: Remove this once KFP is added to dependencies

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    subprocess.call(["pip3 install --user --upgrade {modified_metaflow_git_url}".format(
        modified_metaflow_git_url=MODIFIED_METAFLOW_URL)], shell=True)

    print("\n----------RUNNING: MAIN STEP COMMAND--------------")

    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_AWS_ARN = os.getenv("S3_AWS_ARN")
    S3_AWS_REGION = os.getenv("S3_AWS_REGION")

    define_s3_env_vars = 'export METAFLOW_DATASTORE_SYSROOT_S3="{}" && export METAFLOW_AWS_ARN="{}" ' \
                         '&& export METAFLOW_AWS_S3_REGION="{}"'.format(S3_BUCKET, S3_AWS_ARN, S3_AWS_REGION)
    define_username = 'export USERNAME="kfp-user"' # TODO: Map username to KFP specific user/profile/namespace
    init_cmd = 'python {0} --datastore="s3" --datastore-root="{1}" init --run-id={2} --task-id=0'.format(DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                                                                         S3_BUCKET, kfp_run_id)
    print(f"INIT COMMAND: {init_cmd}")
    final_init_cmd = "{define_username} && {define_s3_env_vars} && {init_cmd}".format(define_username=define_username,
                                                                                      define_s3_env_vars=define_s3_env_vars,
                                                                                      init_cmd=init_cmd)
    
    if step_name == "start":
        execute(final_init_cmd)
        cur_input_path = f"{kfp_run_id}/_parameters/0"
    else:
        cur_input_path = f"{kfp_run_id}/{parent_step_names[0]}/{parent_task_ids[0]}"

    python_cmd = python_cmd_template.format(ds_root=S3_BUCKET, run_id=kfp_run_id, task_id=task_id, input_paths=cur_input_path)
    print("PYTHON COMMAND: ", python_cmd)
    final_run_cmd = "{define_username} && {define_s3_env_vars} && {python_cmd}".format(define_username=define_username,
                                                                                       define_s3_env_vars=define_s3_env_vars,
                                                                                       python_cmd=python_cmd)
    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    proc_output = proc.stdout
    proc_error = proc.stderr

    if len(proc_error) > 1:
        print("_______________STDERR:_____________________________")
        print(proc_error)

    if len(proc_output) > 1:
        print("_______________STDOUT:_____________________________")
        print(proc_output)

    # TODO: Metadata needed for client API to run needs to be persisted outside before return

    print("___________PRODUCING OUTPUTS OF FOREACH STEP__________")
    # print(f"NUM SPLITS PRINT: {parse_stdout_for_numsplits(proc_output)}")
    iterable_length_output = namedtuple('output', ['iterable', 'length', 'task_id'])
    iterable_len = parse_stdout_for_numsplits(proc_output)    
    iterable_indices = list(range(iterable_len))

    print("_______________ Done _________________________________")
    # END is the final step
    if step_name.lower() == 'end':
        print("_______________ FLOW RUN COMPLETE ________________")
    return iterable_length_output(iterable_indices, iterable_len, task_id + 1)
    

def step_op_func(python_cmd_template, step_name: str,
                 code_url: str,
                 kfp_run_id: str,
                 task_id: int = None,
                 split_index: int = None,
                 special_type: str = None,
                 iterable_length: int = None,
                 parent_task_ids: list = None,
                 parent_step_names: list = None) -> int:
    """
    Function used to create a KFP container op (see: `step_container_op`) that corresponds to a single step in the flow.
    """
    import subprocess
    import os
    import json

    def execute(cmd):
        """
        Helper function to run the given command and print `stderr` and `stdout`.
        """
        proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        proc_output = proc.stdout
        proc_error = proc.stderr

        if len(proc_error) > 1:
            print("_____________ STDERR:_____________________________")
            print(proc_error)

        if len(proc_output) > 1:
            print("______________ STDOUT:____________________________")
            print(proc_output)

    MODIFIED_METAFLOW_URL = 'git+https://github.com/zillow/metaflow.git@feature/for_each_flow'
    DEFAULT_DOWNLOADED_FLOW_FILENAME = 'downloaded_flow.py'

    print("\n----------RUNNING: CODE DOWNLOAD from URL---------")
    subprocess.call(
        ["curl -o {downloaded_file_name} {code_url}".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            code_url=code_url)], shell=True)

    print("\n----------RUNNING: KFP Installation---------------")
    subprocess.call(["pip3 install kfp"], shell=True)  # TODO: Remove this once KFP is added to dependencies

    print("\n----------RUNNING: METAFLOW INSTALLATION----------")
    subprocess.call(["pip3 install --user --upgrade {modified_metaflow_git_url}".format(
        modified_metaflow_git_url=MODIFIED_METAFLOW_URL)], shell=True)

    print("\n----------RUNNING: MAIN STEP COMMAND--------------")

    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_AWS_ARN = os.getenv("S3_AWS_ARN")
    S3_AWS_REGION = os.getenv("S3_AWS_REGION")

    define_s3_env_vars = 'export METAFLOW_DATASTORE_SYSROOT_S3="{}" && export METAFLOW_AWS_ARN="{}" ' \
                         '&& export METAFLOW_AWS_S3_REGION="{}"'.format(S3_BUCKET, S3_AWS_ARN, S3_AWS_REGION)
    define_username = 'export USERNAME="kfp-user"' # TODO: Map username to KFP specific user/profile/namespace
    init_cmd = 'python {0} --datastore="s3" --datastore-root="{1}" init --run-id={2} --task-id=0'.format(DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                                                                         S3_BUCKET, kfp_run_id)
    final_init_cmd = "{define_username} && {define_s3_env_vars} && {init_cmd}".format(define_username=define_username,
                                                                                      define_s3_env_vars=define_s3_env_vars,
                                                                                      init_cmd=init_cmd)
    
    print(f"Task id: {task_id}, {type(task_id)}")
    print(f"Special type: {special_type}, {type(special_type)}")
    print(f"Split index: {split_index}, {type(split_index)}")
    print(f"Iterable len: {iterable_length}, {type(iterable_length)}")
    print(f"Parent step names: {parent_step_names}, {type(parent_step_names)}")
    print(f"Parent task ids: {parent_task_ids}, {type(parent_task_ids)}")

    if step_name == "start":
        execute(final_init_cmd)
        cur_input_path = f"{kfp_run_id}/_parameters/0"
        return_val = task_id + 1
    elif special_type == "fanout_linear": # the step immediately following a foreach step
        task_id = task_id + split_index
        cur_input_path = f"{kfp_run_id}/{parent_step_names[0]}/{parent_task_ids[0]} --split-index {split_index}"
        return_val = -1
    elif special_type == "foreach_join": # join step following a foreach step
        range_of_idx = range(int(parent_task_ids[0]), int(parent_task_ids[0]) + iterable_length)
        cur_input_path = f"{kfp_run_id}/{parent_step_names[0]}/:{','.join([str(idx) for idx in (range_of_idx)])}"
        return_val = task_id + 1 # == task_id + iterable_length + 1
    elif special_type == "join":
        cur_input_path = f"{kfp_run_id}/:"
        for parent_task_id, parent in zip(parent_task_ids, parent_step_names):
            cur_input_path += f"{parent}/{parent_task_id},"
        cur_input_path = cur_input_path.strip(',')
        return_val = task_id + 1
    else: # a simple linear step where we simply increment task_id
        cur_input_path = f"{kfp_run_id}/{parent_step_names[0]}/{parent_task_ids[0]}"
        return_val = task_id + 1

    python_cmd = python_cmd_template.format(ds_root=S3_BUCKET, run_id=kfp_run_id, task_id=task_id, input_paths=cur_input_path)
    final_run_cmd = "{define_username} && {define_s3_env_vars} && {python_cmd}".format(define_username=define_username,
                                                                                       define_s3_env_vars=define_s3_env_vars,
                                                                                       python_cmd=python_cmd)
    print("Python command: ", final_run_cmd)
    proc = subprocess.run(final_run_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    proc_output = proc.stdout
    proc_error = proc.stderr

    if len(proc_error) > 1:
        print("_______________STDERR:_____________________________")
        print(proc_error)

    if len(proc_output) > 1:
        print("_______________STDOUT:_____________________________")
        print(proc_output)

    # TODO: Metadata needed for client API to run needs to be persisted outside before return
    return return_val

def addition_container_op():
    """
    Container op that will provide the utility of addition of pipeline parameters.
    """
    additional_op = kfp.components.func_to_container_op(addition_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return additional_op

def step_container_op():
    """
    Container op that corresponds to a step defined in the Metaflow flowgraph.

    Note: The public docker image is a copy of the internal docker image we were using (borrowed from aip-kfp-example).
    """
    step_op = kfp.components.func_to_container_op(step_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return step_op

def foreach_container_op():
    """
    Container op that corresponds to a step defined in the Metaflow flowgraph.

    Note: The public docker image is a copy of the internal docker image we were using (borrowed from aip-kfp-example).
    """
    step_op = kfp.components.func_to_container_op(foreach_op_func, base_image='ssreejith3/mf_on_kfp:python-curl-git')
    return step_op

def add_env_variables_transformer(container_op):
    """
    Add environment variables to the container op.
    """

    container_op.add_env_variable(V1EnvVar(name="S3_BUCKET", value=DATASTORE_SYSROOT_S3))
    container_op.add_env_variable(V1EnvVar(name="S3_AWS_ARN", value=METAFLOW_AWS_ARN))
    container_op.add_env_variable(V1EnvVar(name="S3_AWS_REGION", value=METAFLOW_AWS_S3_REGION))
    return container_op

def distance_to_node(graph, start_step: str, end_step: str, num_levels) -> int:
    """
    Calculates the distance between 2 nodes in the graph. 
    """
    start_node = graph.nodes[start_step]
    if start_step == end_step: 
        return num_levels
    elif start_node == "end":
        return -1
    else:
        for node in start_node.out_funcs:
            distance = distance_to_node(graph, node, end_step, num_levels + 1)
            if distance != -1:
                return distance
    return -1 # if none of the above cases, we return -1 to backtrack

def allowed_to_enqueue(graph, current_step: str, child_step: str) -> bool:
    """
    This function determines whether an output node of the current node 
    can be enqueued in the queue during the level order traversal.
    Please see the following link for a flow that cannot work without this method:
    https://gist.github.com/hsezhiyan/23b8a237e585aa76487b0747b431aca1
    """
    current_node = graph.nodes[current_step]
    for parent_step in current_node.in_funcs:
        parent_node = graph.nodes[parent_step]
        for sibling_step in parent_node.out_funcs:
            if parent_step != sibling_step:
                distance_to_child_step = distance_to_node(graph, sibling_step, child_step, 0)
                if distance_to_child_step > 1:
                    return False
    return True

def create_command_templates_from_graph(graph):
    """
    Create a map of steps to their corresponding command templates. These command templates help define the command
    to be used to run that particular step with placeholders for the `run_id` and `datastore_root` (location of the datastore).

    # Note:
    # Level-order traversal is adopted to keep the task-ids in line with what happens during a local metaflow execution.
    # It is not entirely necessary to keep this order of task-ids if we are able to point to the correct input-paths for
    # each step. But, using this ordering does keep the organization of data in the datastore more understandable and
    # natural (i.e., `start` gets a task id of 1, next step gets a task id of 2 and so on with 'end' step having the
    # highest task id. So the paths in the datastore look like: {run-id}/start/1, {run-id}/next-step/2, and so on)
    """

    # print("Distance: ", distance_to_node(graph, "start", "start", 0))
    # print("Allowed to enqueue: ", allowed_to_enqueue(graph, "join1", "join2"))
    # exit(0)

    def build_cmd_template(step_name):
        """
        Returns the python command template to be used for each step.

        This method returns a string with placeholders for `datastore_root` and `run_id`
        which get populated using the provided config and the kfp run ID respectively.
        The rest of the command string is populated using the passed arguments which are known before the run starts.

        An example constructed command template (to run a step named `hello`):
        "python downloaded_flow.py --datastore s3 --datastore-root {ds_root} " \
                         "step hello --run-id {run_id} --task-id 2 " \
                         "--input-paths {run_id}/start/1"
        """

        python_cmd = "python {downloaded_file_name} --datastore s3 --datastore-root {{ds_root}} " \
                     "step {step_name} --run-id {{run_id}} --task-id {{task_id}} " \
                     "--input-paths {{input_paths}} --with retry".format(downloaded_file_name=DEFAULT_DOWNLOADED_FLOW_FILENAME,
                                                            step_name=step_name)
        return python_cmd

    steps_deque = deque(['start']) # deque to process the DAG in level order
    current_task_id = 0

    # set of seen steps, i.e., added to the queue for processing
    seen_steps = set(['start'])
    # Mapping of steps to task ids
    step_to_task_id_map = {}
    # Mapping of steps to their command templates
    step_to_command_template_map = {}

    while len(steps_deque) > 0:
        current_step = steps_deque.popleft()
        current_task_id += 1
        step_to_task_id_map[current_step] = current_task_id
        current_node = graph.nodes[current_step]

        step_to_command_template_map[current_step] = build_cmd_template(current_step)
        print(f"Step: {current_step}, task ID: {current_task_id}")

        for child_step in current_node.out_funcs:
            if child_step not in seen_steps and allowed_to_enqueue(graph, current_step, child_step):
                steps_deque.append(child_step)
                seen_steps.add(child_step)

    return step_to_command_template_map


def create_kfp_pipeline_from_flow_graph(flow_graph, code_url=DEFAULT_FLOW_CODE_URL):

    step_to_command_template_map = create_command_templates_from_graph(flow_graph)

    @dsl.pipeline(
        name='MF on KFP Pipeline',
        description='Pipeline defining KFP equivalent of the Metaflow flow. Currently supports linear flows and flows '
                    'with branch and join nodes'
    )
    def kfp_pipeline_from_flow():
        kfp_run_id = 'kfp-' + dsl.RUN_ID_PLACEHOLDER
        step_to_container_op_map = {}
        # key: step name: value: container op object. used to deal with multiple foreachs orchestrating at the same level in the graph
        # See https://gist.github.com/hsezhiyan/67000fbfd483e0b7a6ec108e10ea1d52 for an example flow with multiple same-level foreach branches
        foreach_op_storage = {}
        # key: step name, value: pipeline param of associated task_id
        task_id_storage = {}

        # Define container ops for remaining steps
        for step, cmd in step_to_command_template_map.items():

            if step == "start": # we universally start with task_id=1, but it get's updated below
                task_id = 1
        
            task_id_storage[step] = task_id
            parent_step_names = flow_graph.nodes[step].in_funcs # list of strings
            parent_task_ids = [task_id_storage[parent] for parent in parent_step_names] # list of pipelines params

            if flow_graph.nodes[step].type == "foreach": # foreach step
                container_op = (foreach_container_op())(
                                                    step_to_command_template_map[step], step, code_url, kfp_run_id, task_id=task_id,
                                                    parent_task_ids=parent_task_ids, parent_step_names=parent_step_names
                                                ).set_display_name(step)
                task_id = container_op.outputs["task_id"]
                foreach_op_storage[step] = container_op
                print(f"Foreach step: {step}, container: {container_op.name}")
            # fanout linear step
            elif flow_graph.nodes[step].type == "linear" and len(flow_graph.nodes[step].in_funcs) > 0 and flow_graph.nodes[step].in_funcs[0] in foreach_op_storage:
                foreach_op = foreach_op_storage[flow_graph.nodes[step].in_funcs[0]]
                foreach_op_storage[step] = foreach_op # needed for use by the following step
                # TODO: doesn't yet work for multiple successive fanouts
                with kfp.dsl.ParallelFor(foreach_op.outputs["iterable"], parallelism=1) as split_index:
                    container_op = (step_container_op())(
                                                        step_to_command_template_map[step], step, code_url, kfp_run_id, task_id=task_id, special_type="fanout_linear",
                                                        iterable_length=foreach_op.outputs["length"], split_index=split_index, parent_task_ids=parent_task_ids, 
                                                        parent_step_names=parent_step_names
                                                    ).set_display_name(step)
                addition_op = (addition_container_op())([task_id, foreach_op.outputs["length"]])
                task_id = addition_op.output
                print(f"Fanout step: {step}, container: {container_op.name}")
            elif flow_graph.nodes[step].type == "join":
                if len(flow_graph.nodes[step].in_funcs) > 0 and flow_graph.nodes[step].in_funcs[0] in foreach_op_storage:
                    foreach_op = foreach_op_storage[flow_graph.nodes[step].in_funcs[0]]
                    container_op = (step_container_op())(
                                                    step_to_command_template_map[step], step, code_url, kfp_run_id, task_id=task_id, special_type="foreach_join",
                                                    iterable_length=foreach_op.outputs["length"], parent_task_ids=parent_task_ids, parent_step_names=parent_step_names
                                                ).set_display_name(step)
                    print(f"Foreach join step: {step}, container: {container_op.name}")
                else: # standard join step
                    container_op = (step_container_op())(
                                                    step_to_command_template_map[step], step, code_url, kfp_run_id, task_id=task_id, special_type="join",
                                                    parent_task_ids=parent_task_ids, parent_step_names=parent_step_names
                                                ).set_display_name(step)
                    print(f"Normal join step: {step}, container: {container_op.name}")
                task_id = container_op.output
            else: # normal linear step
                container_op = (step_container_op())(
                                                step_to_command_template_map[step], step, code_url, kfp_run_id, task_id=task_id,
                                                parent_task_ids=parent_task_ids, parent_step_names=parent_step_names
                                            ).set_display_name(step)
                task_id = container_op.output
                print(f"Normal step: {step}, container: {container_op.name}")
            
            step_to_container_op_map[step] = container_op

        # Add environment variables to all ops
        dsl.get_pipeline_conf().add_op_transformer(add_env_variables_transformer)

        # Define ordering of container op execution
        for step in flow_graph.nodes.keys():
            if step != 'start':
                for parent in flow_graph.nodes[step].in_funcs:
                    step_to_container_op_map[step].after(step_to_container_op_map[parent])

    return kfp_pipeline_from_flow


def create_run_on_kfp(flow_graph, code_url, experiment_name, run_name, namespace, api_namespace, userid):
    """
    Creates a new run on KFP using the `kfp.Client()`. Note: Intermediate pipeline YAML is not generated as this creates
    the run directly using the pipeline function returned by `create_flow_pipeline`
    """

    pipeline_func = create_kfp_pipeline_from_flow_graph(flow_graph, code_url)
    run_pipeline_result = kfp.Client(namespace=api_namespace, userid=userid).create_run_from_pipeline_func(pipeline_func,
                                                                     arguments={},
                                                                     experiment_name=experiment_name,
                                                                     run_name=run_name,
                                                                     namespace=namespace)
    return run_pipeline_result


def create_kfp_pipeline_yaml(flow_graph, code_url, pipeline_file_path=DEFAULT_KFP_YAML_OUTPUT_PATH):
    """
    Creates a new KFP pipeline YAML using `kfp.compiler.Compiler()`. Note: Intermediate pipeline YAML is saved
    at `pipeline_file_path`
    """
    pipeline_func = create_kfp_pipeline_from_flow_graph(flow_graph, code_url)

    kfp.compiler.Compiler().compile(pipeline_func, pipeline_file_path)
    return pipeline_file_path
