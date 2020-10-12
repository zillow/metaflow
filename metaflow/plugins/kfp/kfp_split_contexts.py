import json
import os
import pprint
from typing import Dict

from metaflow import S3, current
from metaflow.graph import DAGNode
from metaflow.plugins.kfp.kfp_constants import (
    KFP_METAFLOW_CONTEXT_DICT_PATH,
    PASSED_IN_SPLIT_INDEXES,
    SPLIT_SEPARATOR,
)


class KfpSplitContext(object):
    def __init__(self, graph, step_name, run_id, datastore, logger):
        self.graph = graph
        self.step_name = step_name
        self.run_id = run_id
        self.logger = logger
        self.node = graph[step_name]
        self.flow_root = datastore.make_path(graph.name, run_id)

    def build_context_dict(self, flow):
        """
        Returns a dict with
        {
            task_id: The task_id of this step
            foreach_splits: <PASSED_IN_SPLIT_INDEXES>_index where index is ordinal of the split
        }
        """
        if self.node.type == "foreach":
            passed_in_split_indexes = os.environ[PASSED_IN_SPLIT_INDEXES]

            # The splits are fed to kfp.ParallelFor to downstream steps as
            # "passed_in_split_indexes" variable and become the step task_id
            # Example: 0_3_1
            foreach_splits = [
                f"{passed_in_split_indexes}{SPLIT_SEPARATOR}{split_index}".strip(
                    SPLIT_SEPARATOR
                )
                for split_index in range(0, flow._foreach_num_splits)
            ]

            return {
                "task_id": current.task_id,
                "foreach_splits": foreach_splits,
            }
        else:
            return {
                "task_id": current.task_id,
            }

    def get_step_task_id(self, passed_in_split_indexes: str, task_id: str):
        if self.node.is_inside_foreach:
            return passed_in_split_indexes
        else:
            return task_id

    def get_input_context(
        self,
        context_node: str,
        current_node: DAGNode,
        passed_in_split_indexes: str,
    ) -> Dict[str, str]:
        self.logger(context_node, head="--")
        if self.graph[context_node].is_inside_foreach:
            self.logger("is_inside_foreach")
            if (
                self.graph[context_node].type == "foreach"
                and context_node == current_node.in_funcs[0]  # and is a direct parent
            ):
                # if the direct parent node is a foreach
                # and is_inside_foreach then:
                #   it's task_output_path is passed_in_split_indexes
                #   minus the last split_index which is for the inner loop
                self.logger(f"graph[context_node].type {self.graph[context_node].type}")
                split_indices_but_last_one = passed_in_split_indexes.split(
                    SPLIT_SEPARATOR
                )[:-1]
                step_kfp_output_name = (
                    f"{SPLIT_SEPARATOR.join(split_indices_but_last_one)}"
                    f".{context_node}"
                )
            else:
                step_kfp_output_name = f"{passed_in_split_indexes}.{context_node}"
            self.logger(f"context_node {context_node}")
        else:
            step_kfp_output_name = context_node

        path = os.path.join(
            os.path.join(self.flow_root, "step_kfp_outputs"),
            f"{step_kfp_output_name}.json",
        )
        with S3() as s3:
            self.logger(f"get_input_context({step_kfp_output_name} path: {path}")
            input_context = json.loads(s3.get(path).text)
            self.logger(pprint.pformat(input_context), head="--")
            return input_context

    def get_current_step_split_index(self, passed_in_split_indexes: str):
        if self.node.is_inside_foreach:
            # the index is the last appended split ordinal
            return passed_in_split_indexes.split(SPLIT_SEPARATOR)[-1]
        else:
            return passed_in_split_indexes

    @staticmethod
    def save_context_to_local_fs(context_dict):
        # write: context_dict to local FS to return
        with open(KFP_METAFLOW_CONTEXT_DICT_PATH, "w") as file:
            json.dump(context_dict, file)

    def upload_context_to_flow_root(self, context_dict):
        # upload: context_dict to
        #   S3://<self.flow_root>/step_kfp_outputs/<step_kfp_output_name>.json
        with S3() as s3:
            s3_path = self._build_task_output_path()
            s3.put(s3_path, json.dumps(context_dict))
            self.logger("uploaded: " + s3_path)

    def _build_task_output_path(self):
        # task_id when is_inside_foreach is fed to steps via passed_in_split_indexes
        step_name = self.node.name
        step_kfp_output_name = (
            f"{current.task_id}.{step_name}"
            if self.node.is_inside_foreach
            else step_name
        )
        s3_path = os.path.join(
            os.path.join(self.flow_root, "step_kfp_outputs"),
            f"{step_kfp_output_name}.json",
        )
        return s3_path
