"""
Frequently used KFP client interactions, including support for:
- flow triggering flow

Code here exists mainly for user's convenience, to take advantage of Metaflow configs.
They are technically not metaflow code.
"""
import datetime
import json
import logging
import time
from typing import List, Optional, Callable
import posixpath

import math

try:  # Removing hard dependency on KFP for non-KFP plug-in usage
    import kfp
    import kfp_server_api
except:
    pass

from metaflow.metaflow_config import KFP_RUN_URL_PREFIX, KFP_USER_DOMAIN
from metaflow.plugins.kfp.kfp_constants import (
    KFP_CLI_DEFAULT_SORT_BY,
    KFP_CLI_DEFAULT_RETRY,
)
from metaflow.util import get_username


def _get_kfp_client():
    kfp_client_user = get_username()  # TODO: Security concerns?
    if KFP_USER_DOMAIN:
        kfp_client_user += f"@{KFP_USER_DOMAIN}"
    else:  # FIXME: The KFP_USER_DOMAIN value might not be available in cluster
        kfp_client_user += "@zillowgroup.com"
    return kfp.Client(userid=kfp_client_user)


def _retry(func: Callable, max_attempt: int, **kwargs):
    for attempt in range(1, max_attempt + 1):
        try:
            return func(kwargs)
        except Exception as e:
            logging.exception(f"Function {func.__name__} failed on attempt {attempt}")
            if attempt == max_attempt:
                raise e


def get_pipeline_versions(
    kubeflow_pipeline_name: str, sort_by=KFP_CLI_DEFAULT_SORT_BY, verbose=True
) -> List[kfp_server_api.ApiPipeline]:
    """Get kfp pipeline version by name. See get_pipeline_versions_by_id for details."""
    client: kfp.Client = _get_kfp_client()
    kubeflow_pipeline_id = client.get_pipeline_id(kubeflow_pipeline_name)
    return get_pipeline_versions_by_id(
        kubeflow_pipeline_id=kubeflow_pipeline_id, sort_by=sort_by, verbose=verbose
    )


def get_pipeline_versions_by_id(
    kubeflow_pipeline_id: str, sort_by=KFP_CLI_DEFAULT_SORT_BY, verbose=True
) -> List[kfp_server_api.ApiPipeline]:
    """Get kfp pipeline version by id.
    pipeline_id: Pipeline id in corresponding KFP server
    sort_by: Can be format of “field_name”, “field_name asc” or “field_name desc”
      (Example, “name asc” or “id desc”). Ascending by default. See list_pipeline_versions
    verbose: Print pipeline info if True.
      Default to True since this function is intended to be used interactively (human facing).
      For similar reason `print` is used over logging.info to avoid silencing output by default
      logging level.
    """
    client: kfp.Client = _get_kfp_client()
    versions = client.list_pipeline_versions(
        pipeline_id=kubeflow_pipeline_id, sort_by=sort_by
    ).versions

    if verbose:
        print(f"{len(versions)} versions found for pipeline {kubeflow_pipeline_id}.")
        print(f"Versions sorted by {sort_by}.")

        for version_count, version in enumerate(versions, start=1):
            print(f"\n=== Version {version_count} ===")
            for key, value in version.to_dict().items():
                print(f"{key}: {value}")

    return versions


def run_kubeflow_pipeline(
    kubeflow_pipeline_name: str,
    triggered_run_name: str,
    kubeflow_namespace: str,
    kubeflow_experiment_name: Optional[str] = None,
    kubeflow_pipeline_version: Optional[str] = None,
    parameters: Optional[dict] = None,
    max_retry: int = KFP_CLI_DEFAULT_RETRY,
) -> kfp_server_api.ApiRun:
    """Trigger KFP flow by pipeline name. See run_kubeflow_pipeline_by_id for more details."""
    client: kfp.Client = _get_kfp_client()
    kubeflow_pipeline_id: str = client.get_pipeline_id(name=kubeflow_pipeline_name)
    return run_kubeflow_pipeline_by_id(
        kubeflow_pipeline_id=kubeflow_pipeline_id,
        triggered_run_name=triggered_run_name,
        kubeflow_experiment_name=kubeflow_experiment_name,
        kubeflow_namespace=kubeflow_namespace,
        kubeflow_pipeline_version=kubeflow_pipeline_version,
        parameters=parameters,
        max_retry=max_retry,
    )


def run_kubeflow_pipeline_by_id(
    kubeflow_pipeline_id: str,
    triggered_run_name: str,
    kubeflow_namespace: str,
    kubeflow_experiment_name: Optional[str] = None,
    kubeflow_pipeline_version: Optional[str] = None,
    parameters: Optional[dict] = None,
    max_retry: int = KFP_CLI_DEFAULT_RETRY,
) -> kfp_server_api.ApiRun:
    """Trigger KFP flow by pipeline id.
    pipeline_id: Pipeline id for which a new run should be triggered.
    experiment_name: Experiment where the triggered run should be placed.
    job_name: Job name of the flow being triggered.

    Return run id of created run. To reconstruct url see run_id_to_url function.
    """

    def create_experimen():
        return client.create_experiment(
            name=kubeflow_experiment_name,
            description="Experiment flow trigger from same namespace",
            namespace=kubeflow_namespace,  # TODO: Warn about permission issue early
        )

    def run_pipeline():
        return client.run_pipeline(
            experiment_id=experiment.id,
            job_name=triggered_run_name,
            pipeline_id=kubeflow_pipeline_id,
            params={"flow_parameters_json": json.dumps(parameters)},
            version_id=kubeflow_pipeline_version,
        )

    if parameters is None:
        parameters = {}

    client = _get_kfp_client()
    experiment: kfp_server_api.ApiExperiment = _retry(create_experimen, max_retry)
    pipeline_run: kfp_server_api.ApiRun = _retry(run_pipeline, max_retry)
    print(f"Triggered run {pipeline_run.id} - {run_id_to_url(pipeline_run.id)}")
    return pipeline_run


def run_id_to_url(run_id: str):
    return posixpath.join(
        KFP_RUN_URL_PREFIX,
        "_/pipeline/#/runs/details",
        run_id,
    )


def is_finished_run(api_run: kfp_server_api.ApiRun):
    run_status = api_run.status
    return run_status and run_status.lower() in [
        "succeeded",
        "failed",
        "skipped",
        "error",
    ]


def get_kfp_run(run_id, retry=KFP_CLI_DEFAULT_RETRY, client: kfp.Client = None):
    client: kfp.Client = client or _get_kfp_client()
    return _retry(client.get_run, max_attempt=retry, run_id=run_id).run


def wait_for_kfp_run_completion(
    run_id: str,
    wait_timeout: [int, datetime.timedelta] = 0,
    min_check_delay: int = 5,
    max_check_delay: int = 30,
    retry: int = KFP_CLI_DEFAULT_RETRY,
) -> kfp_server_api.ApiRun:
    """Check for KFP run status.

    If timeout (in second) is positive this function waits for flow to complete.
    Raise timeout if run is not finished after <timeout> seconds
    - finished or not (bool)
    - success or not (bool)
    - run info (kfp_server_api.ApiRun)

    TODO before merge: Print v.s. Logging
        - KFP client use logging.info which may be silenced by default
        - metaflow log formatter for splunk digestion?
    """

    def get_delay(secs_since_start, min_delay, max_delay):
        # this sigmoid function reaches
        # - 0.1 after 11 minutes
        # - 0.5 after 15 minutes
        # - 1.0 after 23 minutes
        # in other words, the user will see very frequent updates
        # during the first 10 minutes
        sigmoid = 1.0 / (1.0 + math.exp(-0.01 * secs_since_start + 9.0))
        return min_delay + sigmoid * max_delay

    client: kfp.Client = _get_kfp_client()
    run: kfp_server_api.ApiRun = get_kfp_run(run_id=run_id, retry=retry, client=client)

    if not is_finished_run(run) != "succeeded" and wait_timeout:
        if isinstance(wait_timeout, datetime.timedelta):
            wait_timeout = wait_timeout.total_seconds()

        # A mimic of kfp.Client.wait_for_run_completion with customized print
        print(f"Waiting for run {run_id} to finish. Timeout: {wait_timeout} second(s)")
        start_time = datetime.datetime.now()
        while not is_finished_run(run):
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            print(
                f"Waiting for the run {run_id} to complete... {elapsed_time}s / {wait_timeout}s"
            )
            if elapsed_time > wait_timeout:
                raise TimeoutError(f"Timeout while waiting for run {run_id} to finish.")
            time.sleep(
                get_delay(
                    elapsed_time, min_delay=min_check_delay, max_delay=max_check_delay
                )
            )
            run = get_kfp_run(run_id=run_id, retry=retry, client=client)

    return run


def terminate_run(run_id: str, retry: int = KFP_CLI_DEFAULT_RETRY, **kwargs):
    run_service_api = kfp_server_api.RunServiceApi()
    return _retry(run_service_api.terminate_run, retry, run_id=run_id, **kwargs)
