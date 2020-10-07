import inspect
import os
import random
import string
import sys
from pathlib import Path
from typing import NamedTuple, Dict, Union

import kfp

from metaflow.metaflow_config import DATASTORE_SYSROOT_S3
from kfp.dsl import ContainerOp

from .constants import DEFAULT_KFP_YAML_OUTPUT_PATH, SPLIT_SEPARATOR
from ... import R, S3
from ...graph import DAGNode, FlowGraph
from ...plugins.resources_decorator import ResourcesDecorator

STEP_INIT_SH = "step-init.sh"


class KfpComponent(object):
    def __init__(self, name, step_command, total_retries):
        self.name = name
        self.step_command = step_command
        self.total_retries = total_retries


class KubeflowPipelines(object):
    def __init__(
        self,
        name,
        graph,
        flow,
        code_package,
        code_package_url,
        metadata,
        datastore,
        environment,
        event_logger,
        monitor,
        base_image=None,
        s3_code_package=True,
        namespace=None,
        api_namespace=None,
        username=None,
        **kwargs,
    ):
        """
        Analogous to step_functions_cli.py
        """
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package = code_package
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.datastore = datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.namespace = namespace
        self.username = username
        self.base_image = base_image
        self.s3_code_package = s3_code_package

        self._client = kfp.Client(namespace=api_namespace, userid=username, **kwargs)

    def create_run_on_kfp(self, experiment_name, run_name):
        """
        Creates a new run on KFP using the `kfp.Client()`.
        """
        run_pipeline_result = self._client.create_run_from_pipeline_func(
            pipeline_func=self.create_kfp_pipeline_from_flow_graph(),
            arguments={"datastore_root": DATASTORE_SYSROOT_S3},
            experiment_name=experiment_name,
            run_name=run_name,
            namespace=self.namespace,
        )
        return run_pipeline_result

    def create_kfp_pipeline_yaml(self, pipeline_file_path=DEFAULT_KFP_YAML_OUTPUT_PATH):
        """
        Creates a new KFP pipeline YAML using `kfp.compiler.Compiler()`.
        Note: Intermediate pipeline YAML is saved at `pipeline_file_path`
        """
        kfp.compiler.Compiler().compile(
            self.create_kfp_pipeline_from_flow_graph(), pipeline_file_path
        )
        return os.path.abspath(pipeline_file_path)

    def _command(self, code_package_url, environment, step_name, step_cli, task_id):
        """
        Analogous to batch.py
        """
        commands = (
            environment.get_package_commands(code_package_url)
            if self.s3_code_package
            else ["cd " + str(Path(inspect.getabsfile(self.flow.__class__)).parent)]
        )
        commands.extend(environment.bootstrap_commands(step_name))
        commands.append("echo 'Task is starting.'")
        commands.extend(step_cli)
        subshell_commands = " && ".join(
            commands
        )  # run inside subshell to capture all stdout/stderr
        # redirect stdout/stderr to separate files, using tee to display to UI
        redirection_commands = "> >(tee -a 0.stdout.log) 2> >(tee -a 0.stderr.log >&2)"

        # Creating a template to save logs to S3. This is within a function because
        # datastore_root is not available within the scope of this function, and needs
        # to be provided in the `step_op` function. f strings (AFAK) don't support
        # insertion of only a partial number of placeholder strings.
        def create_log_cmd(log_file):
            save_logs_cmd_template = (
                f"python -m awscli s3 cp {log_file} {{datastore_root}}/"
                f"{self.flow.name}/{{run_id}}/{step_name}/"
                f"{task_id}/{log_file}"
            )
            return save_logs_cmd_template

        log_stdout_cmd = create_log_cmd(log_file="0.stdout.log")
        log_stderr_cmd = create_log_cmd(log_file="0.stderr.log")
        save_logs_cmd = f"{log_stderr_cmd} >/dev/null && {log_stdout_cmd} >/dev/null"

        # We capture the exit code at two places:
        # Once after the subshell/redirection commands, and once after the saving logs
        # command. If either of these exit codes are not 0, we exit with the nonzero
        # exit code manually because combining bash commands with ';' always results
        # in an exit code of 0, whether or not certain commands failed.
        return (
            f"({subshell_commands}) {redirection_commands}; export exit_code_1=$?; "
            f"{save_logs_cmd}; export exit_code_2=$?; "
            f'if [ "$exit_code_1" -ne 0 ]; then exit $exit_code_1; else exit $exit_code_2; fi'
        )

    @staticmethod
    def _get_retries(node):
        """
        Analogous to step_functions_cli.py
        """
        max_user_code_retries = 0
        max_error_retries = 0
        # Different decorators may have different retrying strategies, so take
        # the max of them.
        for deco in node.decorators:
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        return max_user_code_retries, max_user_code_retries + max_error_retries

    @staticmethod
    def _get_resource_requirements(node):
        """
        Get resource request or limit for a Metaflow step (node) set by @resources decorator.

        Supported parameters: 'cpu', 'cpu_limit', 'gpu', 'gpu_vendor', 'memory', 'memory_limit'
        Keys with no suffix set resource request (minimum);
        keys with 'limit' suffix sets resource limit (maximum).

        Eventually resource request and limits link back to kubernetes, see
        https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/

        Default unit for memory is megabyte, aligning with existing resource decorator usage.

        Example using resource decorator:
            @resource(cpu=0.5, cpu_limit=2, gpu=1, memory=300)
            @step
            def my_kfp_step(): ...
        """

        def to_k8s_resource_format(resource: str, value: Union[int, float, str]):
            value = str(value)

            # Defaults memory unit to megabyte
            if resource in ["memory", "memory_limit"] and value.isnumeric():
                value = f"{value}M"
            return value

        resource_requirement = dict()
        for deco in node.decorators:
            if isinstance(deco, ResourcesDecorator):
                for attr_key, attr_value in deco.attributes.items():
                    if attr_value is not None:
                        resource_requirement[attr_key] = to_k8s_resource_format(
                            attr_key, attr_value
                        )

        return resource_requirement

    def create_kfp_components_from_graph(self):
        """
        Returns a map of steps to their corresponding KfpComponent.
        The KfpComponent defines the component attributes
        and step command to be used to run that particular step.
        """

        def build_kfp_component(node: DAGNode, step_name: str, task_id: int):
            """
            Returns the KfpComponent for each step.

            This method returns a string with placeholders for `run_id` and
            `task_id` which get populated using the provided config and the kfp
            run ID respectively.  The rest of the command string is populated
            using the passed arguments which are known before the run starts.

            An example constructed command template (to run a step named `hello`):
            "python downloaded_flow.py --datastore s3 --datastore-root {ds_root} " \
                             "step hello --run-id {run_id} --task-id 2 " \
                             "--input-paths {run_id}/start/1"
            """

            # TODO: @schedule, @environment, @resources, @timeout, @catch, etc.
            # Resolve retry strategy.
            user_code_retries, total_retries = KubeflowPipelines._get_retries(node)

            step_cli = self._step_cli(node, task_id, user_code_retries)

            return KfpComponent(
                node.name,
                self._command(
                    self.code_package_url,
                    self.environment,
                    step_name,
                    [step_cli],
                    task_id,
                ),
                total_retries,
            )

        # Mapping of steps to their KfpComponent
        step_to_kfp_component_map = {}
        steps_queue = ["start"]  # Queue to process the DAG in level order
        seen_steps = {"start"}  # Set of seen steps
        task_id = 0
        while len(steps_queue) > 0:
            current_step = steps_queue.pop(0)
            node = self.graph.nodes[current_step]
            task_id += 1

            step_to_kfp_component_map[node.name] = build_kfp_component(
                node, node.name, task_id
            )

            for step in node.out_funcs:
                if step not in seen_steps:
                    steps_queue.append(step)
                    seen_steps.add(step)

        return step_to_kfp_component_map

    def _step_cli(self, node: DAGNode, task_id: int, user_code_retries):
        """
        Analogous to step_functions_cli.py
        This returns the command line to run the internal Metaflow step click entrypiont.
        """
        from kfp import dsl

        cmds = []

        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)

        if R.use_r():
            entrypoint = [R.entrypoint()]
        else:
            entrypoint = [executable, script_name]

        kfp_run_id = "kfp-" + dsl.RUN_ID_PLACEHOLDER
        start_task_id_params_path = None

        if node.name == "start":
            # We need a separate unique ID for the special _parameters task
            task_id_params = "1-params"
            # Export user-defined parameters into runtime environment
            param_file = "".join(
                random.choice(string.ascii_lowercase) for _ in range(10)
            )

            # TODO: move to KFP plugin
            export_params = (
                "python -m "
                "metaflow.plugins.aws.step_functions.set_batch_environment "
                "parameters %s && . `pwd`/%s" % (param_file, param_file)
            )
            params = entrypoint + [
                "--quiet",
                "--metadata=%s" % self.metadata.TYPE,
                "--environment=%s" % self.environment.TYPE,
                "--datastore=s3",
                "--datastore-root={datastore_root}",
                "--event-logger=%s" % self.event_logger.logger_type,
                "--monitor=%s" % self.monitor.monitor_type,
                "--no-pylint",
                "init",
                "--run-id %s" % kfp_run_id,
                "--task-id %s" % task_id_params,
            ]

            # If the start step gets retried, we must be careful not to
            # regenerate multiple parameters tasks. Hence we check first if
            # _parameters exists already.
            start_task_id_params_path = (
                "{kfp_run_id}/_parameters/{task_id_params}".format(
                    kfp_run_id=kfp_run_id, task_id_params=task_id_params
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
            "--metadata=%s" % self.metadata.TYPE,
            "--environment=%s" % self.environment.TYPE,
            "--datastore=s3",
            "--datastore-root={datastore_root}",
            "--event-logger=%s" % self.event_logger.logger_type,
            "--monitor=%s" % self.monitor.monitor_type,
            "--no-pylint",
        ]

        cmds.append(
            " ".join(
                entrypoint
                + top_level
                + [
                    "kfp step-init",
                    "--run-id %s" % kfp_run_id,
                    "--step_name %s" % node.name,
                    "--split_index {split_index}",
                    "--task_id %s" % task_id,  # the assigned task_id from Flow graph
                ]
            )
        )

        # load environment variables set in STEP_INIT_SH
        cmds.append(f". `pwd`/{STEP_INIT_SH}")

        step = [
            "--with=kfp_internal",
            "step",
            node.name,
            "--run-id %s" % kfp_run_id,
            f"--task-id $TASK_ID",
            # Since retries are handled by KFP Argo, we can rely on
            # {{retries}} as the job counter.
            # '--retry-count {{retries}}',  # TODO: test verify, should it be -1?
            "--max-user-code-retries %d" % user_code_retries,
            (
                "--input-paths %s" % start_task_id_params_path
                if node.name == "start"
                else "--input-paths $INPUT_PATHS"
            ),
        ]

        if any(self.graph[n].type == "foreach" for n in node.in_funcs):
            # We set the `METAFLOW_SPLIT_INDEX` through JSONPath-foo
            # to pass the state from the parent DynamoDb state for for-each.
            step.append("--split-index $SPLIT_INDEX")  # TODO: get from KFP param
        if self.namespace:
            step.append("--namespace %s" % self.namespace)

        cmds.append(" ".join(entrypoint + top_level + step))
        return " && ".join(cmds)

    def create_kfp_pipeline_from_flow_graph(self):
        import kfp
        from kfp import dsl

        step_to_kfp_component_map = self.create_kfp_components_from_graph()

        # Container op that corresponds to a step defined in the Metaflow flowgraph.
        step_op = kfp.components.func_to_container_op(
            step_op_func, base_image=self.base_image
        )

        def pipeline_transform(op):
            # op.execution_options.caching_strategy.max_cache_staleness = "P0D"
            op.container.set_cpu_request("50m")
            op.container.set_cpu_limit("250m")

        @dsl.pipeline(name=self.name, description=self.graph.doc)
        def kfp_pipeline_from_flow(datastore_root: str = DATASTORE_SYSROOT_S3):
            kfp_run_id = "kfp-" + dsl.RUN_ID_PLACEHOLDER

            visited = {}

            def set_resource_requirements(
                container_op: ContainerOp, resource_requirements: dict
            ):
                if "memory" in resource_requirements:
                    container_op.container.set_memory_request(
                        resource_requirements["memory"]
                    )
                if "memory_limit" in resource_requirements:
                    container_op.container.set_memory_limit(
                        resource_requirements["memory_limit"]
                    )
                if "cpu" in resource_requirements:
                    container_op.container.set_cpu_request(resource_requirements["cpu"])
                if "cpu_limit" in resource_requirements:
                    container_op.container.set_cpu_limit(
                        resource_requirements["cpu_limit"]
                    )
                if "gpu" in resource_requirements:
                    # TODO(yunw)(AIP-2048): Support mixture of GPU from different vendors.
                    container_op.container.set_gpu_limit(
                        resource_requirements["gpu"],
                        vendor=resource_requirements["gpu_vendor"],
                    )

            def build_kfp_dag(node: DAGNode, split_index=None):
                if node.name in visited:
                    return

                op = step_op(
                    datastore_root,
                    step_to_kfp_component_map[node.name].step_command,
                    kfp_run_id,
                    split_index,
                ).set_display_name(node.name)

                set_resource_requirements(
                    op,
                    KubeflowPipelines._get_resource_requirements(node),
                )

                visited[node.name] = op

                if node.type == "foreach":
                    with kfp.dsl.ParallelFor(op.outputs["foreach_splits"]) as index:
                        inner_op = build_kfp_dag(self.graph[node.out_funcs[0]], index)

                    build_kfp_dag(self.graph[node.matching_join], split_index)
                else:
                    for step in node.out_funcs:
                        step_node = self.graph[step]
                        if (
                            step_node.type == "join"
                            and self.graph[step_node.split_parents[-1]].type
                            == "foreach"
                        ):
                            print(f"--- skipping {step} join base case")
                        else:
                            build_kfp_dag(step_node, split_index)
                return op

            build_kfp_dag(self.graph["start"])

            import pprint

            pprint.pprint(visited.keys())

            for step in self.graph.nodes:
                node = self.graph[step]
                for parent_step in node.in_funcs:
                    visited[node.name].after(visited[parent_step])

            dsl.get_pipeline_conf().add_op_transformer(pipeline_transform)

        return kfp_pipeline_from_flow


def step_op_func(
    datastore_root: str,
    cmd_template: str,
    kfp_run_id: str,
    split_index: str = "",  # only if is_inside_foreach
) -> NamedTuple(
    "split_parent_tasks_ids", [("task_out_dict", dict), ("foreach_splits", list)]
):
    """
    Renders and runs the cmd_template containing Metaflow step/init commands to
    run within the container.
    """
    import os
    import json
    from subprocess import Popen, PIPE, STDOUT
    from typing import NamedTuple

    # TODO: can this be moved to constants?
    KFP_METAFLOW_SPLIT_INDEX_PATH = "/tmp/kfp_metaflow_split_index.json"
    KFP_METAFLOW_OUT_DICT_PATH = "/tmp/kfp_metaflow_out_dict.json"

    if len(split_index) > 0:
        print("======== split_index")
        print(split_index)
        print("-----------------------------------\n")

    # for the kfp_decorator:
    # TODO: could this be passed in as a kfp cli parameter
    #   and set in the cli obj state?
    with open(KFP_METAFLOW_SPLIT_INDEX_PATH, "w") as file:
        file.write(split_index)

    cmd = cmd_template.format(
        run_id=kfp_run_id,
        datastore_root=datastore_root,
        split_index=split_index if len(split_index) > 0 else '""',
    )

    print("RUNNING COMMAND: ", cmd)
    print("----")
    print(cmd.replace(" && ", "\n"))
    print("----")

    # TODO: Map username to KFP specific user/profile/namespace
    with Popen(
        cmd,
        shell=True,
        universal_newlines=True,
        executable="/bin/bash",
        env=dict(
            os.environ,
            USERNAME="kfp-user",
            METAFLOW_RUN_ID=kfp_run_id,
            METAFLOW_DATASTORE_SYSROOT_S3=datastore_root,
        ),
    ) as process:
        print("Running command.")

    if process.returncode != 0:
        raise Exception("Returned: %s" % process.returncode)

    # written by kfp_decorator.py:task_finished
    with open(KFP_METAFLOW_OUT_DICT_PATH, "r") as file:
        task_out_dict = json.load(file)

    print("___DONE___")

    StepMetaflowContext = NamedTuple(
        "split_parent_tasks_ids", [("task_out_dict", dict), ("foreach_splits", list)]
    )
    return StepMetaflowContext(task_out_dict, task_out_dict.get("foreach_splits", None))


def _cmd_params(
    datastore,
    graph: FlowGraph,
    run_id: str,
    step_name: str,
    split_index: str,
    task_id: str,
):
    """
    non-join nodes: "run-id/parent-step/parent-task-id",
    branch-join node: "run-id/:p1/p1-task-id,p2/p2-task-id,..."
    foreach-split node:
        --input-paths 1/start/1
        --split-index 0 (use --split-index 1,2 and 3 to cover the remaining splits)
    foreach-join node:
        run-id/foreach-parent/:2,3,4,5
        (where 2,3,4,5 are the task_ids of the explore steps that were previously executed)
    """
    import json
    import pprint

    node: DAGNode = graph[step_name]
    flow_root = datastore.make_path(graph.name, run_id)

    environment_exports = {
        # get last int as index
        "SPLIT_INDEX": (
            split_index.split(SPLIT_SEPARATOR)[-1]
            if node.is_inside_foreach
            else split_index
        ),
        "TASK_ID": split_index if node.is_inside_foreach else task_id,
    }

    print("**** environment_exports")
    pprint.pprint(environment_exports)
    print()

    def get_context(context_node: str) -> Dict:
        print("--", context_node)
        if graph[context_node].is_inside_foreach:
            print("is_inside_foreach")
            if (
                graph[context_node].type == "foreach"
                and context_node == node.in_funcs[0]
            ):
                # context_node is a nested foreach!
                # remove last split index to get foreach parent
                print("graph[context_node].type", graph[context_node].type)
                context_node = (
                    f"{SPLIT_SEPARATOR.join(split_index.split(SPLIT_SEPARATOR)[:-1])}"
                    + f".{context_node}"
                )
            else:
                context_node = f"{split_index}.{context_node}"
            print("context_node", context_node)

        path = os.path.join(
            os.path.join(flow_root, "step_kfp_outputs"), f"{context_node}.json"
        )
        with S3() as s3:
            print(f"get_context({context_node} path: {path}")
            parent_context = json.loads(s3.get(path).text)
            pprint.pprint(parent_context)
            print("--")
            return parent_context

    input_paths = f"{run_id}"
    if node.type == "join":
        # load from s3 the context outs foreach
        if graph[node.split_parents[-1]].type == "foreach":
            parent_context = get_context(node.split_parents[-1])
            parent_step_name = node.in_funcs[0]
            input_paths += f"/{parent_step_name}/:"
            parent_task_ids = [split for split in parent_context["foreach_splits"]]
            input_paths += ",".join(parent_task_ids)
        else:
            input_paths += "/:"
            for in_func in node.in_funcs:
                parent_context = get_context(in_func)
                input_paths += (
                    f"{parent_context['step_name']}/{parent_context['task_id']},"
                )
    else:
        if step_name != "start":
            parent_context = get_context(node.in_funcs[0])
            input_paths += f"/{parent_context['step_name']}/{parent_context['task_id']}"

    environment_exports["INPUT_PATHS"] = input_paths.strip(",")

    print("**** step-init")
    pprint.pprint(environment_exports)
    print()

    with open("%s" % STEP_INIT_SH, "w") as file:
        for key, value in environment_exports.items():
            file.write(f"export {key}={value}\n")
    os.chmod(STEP_INIT_SH, 509)
