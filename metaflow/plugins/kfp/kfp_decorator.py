import json
import os
import tarfile
from urllib.parse import urlparse

from metaflow import util, current, S3
from metaflow.datastore import MetaflowDataStore
from metaflow.datastore.util.s3util import get_s3_client
from metaflow.decorators import StepDecorator
from metaflow.graph import DAGNode
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins.kfp.constants import (
    KFP_METAFLOW_OUT_DICT_PATH,
    KFP_METAFLOW_SPLIT_INDEX_PATH,
    SPLIT_SEPARATOR,
)


class KfpInternalDecorator(StepDecorator):
    name = "kfp_internal"

    def task_pre_step(
        self,
        step_name,
        datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
    ):
        """
        Analogous to step_functions_decorator.py
        """
        # TODO: any other KFP environment variables to get and register to Metadata service?
        meta = {"kfp-execution": run_id}
        entries = [MetaDatum(field=k, value=v, type=k) for k, v in meta.items()]
        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)

        if metadata.TYPE == "local":
            self.ds_root = datastore.root
        else:
            self.ds_root = None

        self.flow_root = datastore.make_path(graph.name, run_id)

    def _get_split_index(self):
        if os.stat(KFP_METAFLOW_SPLIT_INDEX_PATH).st_size > 0:
            with open(KFP_METAFLOW_SPLIT_INDEX_PATH, "r") as file:
                return file.readline()
        else:
            return ""

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        """
        Analogous to step_functions_decorator.py
        """
        if not is_task_ok:
            # The task finished with an exception - execution won't
            # continue so no need to do anything here.
            return

        # For foreaches, we need to dump the cardinality of the fanout
        # for the KFP parallelFor
        context_dict = dict(
            step_name=step_name,
            task_id=current.task_id,
            flow_root=self.flow_root,
            node_type=graph[step_name].type,
        )

        prev_split_index = self._get_split_index()
        print("prev_split_index", prev_split_index)

        node: DAGNode = graph[step_name]
        if node.type == "foreach":
            # context_dict["foreach_num_splits"] = flow._foreach_num_splits
            splits = [
                f"{prev_split_index}{SPLIT_SEPARATOR}{i}".strip(
                    SPLIT_SEPARATOR
                )  # downstream next step taskId
                for i in range(0, flow._foreach_num_splits)
            ]
            context_dict["foreach_splits"] = splits
        elif node.is_inside_foreach:
            context_dict["foreach_splits"] = prev_split_index

        import pprint

        print(step_name, context_dict)
        pprint.pprint(context_dict)

        # write: context_dict to local fs
        with open(KFP_METAFLOW_OUT_DICT_PATH, "w") as file:
            json.dump(context_dict, file)

        # task_id when is_inside_foreach is passed in split_index
        step_kfp_output_name = (
            f"{current.task_id}.{step_name}" if node.is_inside_foreach else step_name
        )

        # upload: context_dict to
        #   S3://<self.flow_root>/step_kfp_outputs/<step_kfp_output_name>.json
        with open(KFP_METAFLOW_OUT_DICT_PATH, "rb") as file:
            s3_path = os.path.join(
                os.path.join(self.flow_root, "step_kfp_outputs"),
                f"{step_kfp_output_name}.json",
            )
            with S3() as s3:
                s3.put(s3_path, json.dumps(context_dict))
                print("uploaded: " + s3_path)

        if self.ds_root:
            # We have a local metadata service so we need to persist it to the datastore.
            # Note that the datastore is *always* s3 (see runtime_task_created function)
            with util.TempDir() as td:
                tar_file_path = os.path.join(td, "metadata.tgz")
                with tarfile.open(tar_file_path, "w:gz") as tar:
                    # The local metadata is stored in the local datastore
                    # which, for batch jobs, is always the DATASTORE_LOCAL_DIR
                    tar.add(DATASTORE_LOCAL_DIR)
                # At this point we upload what need to s3
                s3, _ = get_s3_client()
                with open(tar_file_path, "rb") as f:
                    path = os.path.join(
                        self.ds_root,
                        MetaflowDataStore.filename_with_attempt_prefix(
                            "metadata.tgz", retry_count
                        ),
                    )
                    url = urlparse(path)
                    s3.upload_fileobj(f, url.netloc, url.path.lstrip("/"))
                    print("uploaded: " + path)
