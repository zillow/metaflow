"""
Frequently used KFP client interactions, including support for:
- flow triggering flow

Code here exists mainly for user's convenience, to take advantage of Metaflow configs.
They are technically not metaflow code.
"""

import json
import logging
import time
from typing import List
import posixpath

import kfp
import kfp_server_api

from metaflow.metaflow_config import KFP_RUN_URL_PREFIX, KFP_USER_DOMAIN
from metaflow.plugins.kfp.kfp_constants import KFP_CLI_DEFAULT_SORT_BY
from metaflow.util import get_username


def _get_kfp_client():
    kfp_client_user = get_username()
    if KFP_USER_DOMAIN:
        kfp_client_user += f"@{KFP_USER_DOMAIN}"
    else:  # FIXME: The KFP_USER_DOMAIN value might not be available in cluster
        kfp_client_user += "@zillowgroup.com"
    return kfp.Client(userid=kfp_client_user)


def get_pipeline_versions(
    pipeline_name: str, sort_by=KFP_CLI_DEFAULT_SORT_BY, verbose=True
) -> List[kfp_server_api.ApiPipeline]:
    """Get kfp pipeline version by name. See get_pipeline_versions_by_id for details."""
    client = _get_kfp_client()
    pipeline_id = client.get_pipeline_id(pipeline_name)
    return get_pipeline_versions_by_id(pipeline_id, sort_by=sort_by, verbose=verbose)


def get_pipeline_versions_by_id(
    pipeline_id: str, sort_by=KFP_CLI_DEFAULT_SORT_BY, verbose=True
) -> List[kfp_server_api.ApiPipeline]:
    """Get kfp pipeline version by id.
    pipeline_id: Pipeline id in corresponding KFP server
    sort_by: Can be format of “field_name”, “field_name asc” or “field_name desc”
      (Example, “name asc” or “id desc”). Ascending by default.
    verbose: Print pipeline info if True.
      Default to True since this function is intended to be used interactively (human facing).
      For similar reason `print` is used over logging.info to avoid silencing output by default
      logging level.
    """
    client = _get_kfp_client()
    versions = client.list_pipeline_versions(
        pipeline_id=pipeline_id, sort_by=sort_by
    ).versions

    if verbose:
        print(f"{len(versions)} versions found for pipeline {pipeline_id}.")
        print(f"Versions sorted by {sort_by}.")

        for version_count, version in enumerate(versions, start=1):
            print(f"\n=== Version {version_count} ===")
            for key, value in version.to_dict().items():
                print(f"{key}: {value}")

    return versions


def trigger_flow(
    pipeline_name: str,
    triggerred_flow_name: str,
    namespace: str,
    experiment_name: str = None,
    pipeline_parameters: dict = None,
    pipeline_version: str = None,
) -> str:
    """Trigger KFP flow by pipeline name. See trigger_flow_by_id for more details."""

    client = _get_kfp_client()

    pipeline_id = client.get_pipeline_id(name=pipeline_name)
    return trigger_flow_by_id(
        pipeline_id=pipeline_id,
        experiment_name=experiment_name,
        namespace=namespace,
        triggerred_flow_name=triggerred_flow_name,
        pipeline_parameters=pipeline_parameters,
        pipeline_version=pipeline_version,
    )


def trigger_flow_by_id(
    pipeline_id: str,
    triggerred_flow_name: str,
    namespace: str,
    experiment_name: str = None,
    pipeline_parameters: dict = None,
    pipeline_version: str = None,
) -> str:
    """Trigger KFP flow by pipeline id.
    pipeline_id: Pipeline id for which a new run should be triggered.
    experiment_name: Experiment where the triggered run should be placed.
    job_name: Job name of the flow being triggered.

    Return run id of created run. To reconstruct url see run_id_to_url function.
    """
    client = _get_kfp_client()

    if pipeline_parameters is None:
        pipeline_parameters = {}

    # Not checking for experiment existence: kfp client does not recreate experiment if exists
    experiment: kfp_server_api.ApiExperiment = client.create_experiment(
        name=experiment_name,
        description="Experiment flow trigger from same namespace",
        namespace=namespace,  # TODO: Warn about permission issue early
    )

    pipeline_run: kfp_server_api.ApiRun = client.run_pipeline(
        experiment_id=experiment.id,
        job_name=triggerred_flow_name,
        pipeline_id=pipeline_id,
        params={"flow_parameters_json": json.dumps(pipeline_parameters)},
        version_id=pipeline_version,
    )

    return pipeline_run.id


def run_id_to_url(run_id: str):
    return posixpath.join(
        KFP_RUN_URL_PREFIX,
        "_/pipeline/#/runs/details",
        run_id,
    )


def check_kfp_run_status(
    run_id: str, timeout: int = -1
) -> (bool, bool, kfp_server_api.ApiRun):
    """Check for KFP run status.

    If timeout (in second) is positive integers this function waits for flow to complete first.
    Return tuple containing
    - finished or not (bool)
    - success or not (bool)
    - run info (kfp_server_api.ApiRun)

    TODO before merge: Print v.s. Logging
        - KFP client use logging.info which may be silenced by default
        - metaflow log formatter for splunk digestion?
    """
    client: kfp.Client = _get_kfp_client()
    if timeout > 0:
        print(f"Waiting for run {run_id} to finish. Timeout: {timeout}")
        run: kfp_server_api.ApiRun = client.wait_for_run_completion(
            run_id, timeout=timeout
        ).run
    else:
        run: kfp_server_api.ApiRun = client.get_run(run_id).run

    finished = run.status.lower() in ["succeeded", "failed", "skipped", "error"]
    succeeded = run.status.lower() == "succeeded"

    print(
        f"Run {run_id} {'finished' if finished else 'did not finish'} with status {run.status}"
    )

    return succeeded, finished, run
