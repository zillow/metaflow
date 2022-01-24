"""
Frequently used KFP client interactions, including support for:
- flow triggering flow

Code here exists mainly for user's convenience, to take advantage of Metaflow configs.
They are technically not metaflow code.

TODO:
    - (yunw)(AIP-5671): Async version for trigger and wait
    - (yunw)(AIP-5672): Helper function for user to check access to a certain namespace.
      potentially using `kubectl auth can-i`
"""
import datetime
import json
import logging
import math
import posixpath
import sys
import time
from typing import Callable, List, Optional

try:  # Extra required dependency specific to kfp plug-in may not exists
    from kfp import Client as KFPClient
    from kfp_server_api import ApiExperiment, ApiPipeline, ApiRun, RunServiceApi
except ImportError:  # Silence import errors in type hint
    KFPClient = None
    ApiPipeline = None
    ApiRun = None
    RunServiceApi = None

from metaflow.metaflow_config import KFP_RUN_URL_PREFIX, KFP_USER_DOMAIN
from metaflow.plugins.kfp.kfp_constants import (
    KFP_CLI_DEFAULT_RETRY,
    KFP_CLI_DEFAULT_SORT_BY,
)
from metaflow.util import get_username


def get_kfp_logger():
    """Setup logger for KFP plugin

    With expected usage from Jupyter notebook and console in mind,
    INFO level logs need to show up in Jupyter notebook output cell and consoles.
    Therefore some default setting below:
        - Default logging level at logging.DEBUG
        - Default to a StreamHandler pointing to stdout.
    Users retain the ability to alter logger behavior using logging module.
    """
    kfp_logger = logging.getLogger("metaflow.kfp")
    if not kfp_logger.hasHandlers():
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        kfp_logger.addHandler(stdout_handler)
    kfp_logger.setLevel(logging.DEBUG)
    return kfp_logger


logger = get_kfp_logger()


def _get_kfp_client():
    kfp_client_user = get_username()
    if KFP_USER_DOMAIN:
        kfp_client_user += f"@{KFP_USER_DOMAIN}"
    else:  # FIXME: The KFP_USER_DOMAIN value might not be available in cluster
        kfp_client_user += "@zillowgroup.com"
    return KFPClient(userid=kfp_client_user)


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
) -> List[ApiPipeline]:
    """Get kfp pipeline version by name. See get_pipeline_versions_by_id for details."""
    client: KFPClient = _get_kfp_client()
    kubeflow_pipeline_id = client.get_pipeline_id(kubeflow_pipeline_name)
    return get_pipeline_versions_by_id(
        kubeflow_pipeline_id=kubeflow_pipeline_id, sort_by=sort_by, verbose=verbose
    )


def get_pipeline_versions_by_id(
    kubeflow_pipeline_id: str, sort_by=KFP_CLI_DEFAULT_SORT_BY, verbose=True
) -> List[ApiPipeline]:
    """Get kfp pipeline version by id.

    kubeflow_pipeline_id: Pipeline id in corresponding KFP server
    sort_by: Can be format of “field_name”, “field_name asc” or “field_name desc”
        (Example, “name asc” or “id desc”). Ascending by default. See list_pipeline_versions
    verbose: Log pipeline info if True.
        Default to True since this function is intended to be used interactively (human facing).
    """
    client: KFPClient = _get_kfp_client()
    versions = client.list_pipeline_versions(
        pipeline_id=kubeflow_pipeline_id, sort_by=sort_by
    ).versions

    if verbose:
        logger.info(
            f"{len(versions)} versions found for pipeline {kubeflow_pipeline_id}."
        )
        logger.info(f"Versions sorted by {sort_by}.")

        for version_count, version in enumerate(versions, start=1):
            logger.info(f"\n=== Version {version_count} ===")
            for key, value in version.to_dict().items():
                logger.info(f"{key}: {value}")

    return versions


def run_kubeflow_pipeline(
    pipeline_name: str,
    kubeflow_namespace: str,
    triggered_run_name: str,
    kubeflow_experiment_name: Optional[str] = None,
    pipeline_version_id: Optional[str] = None,
    parameters: Optional[dict] = None,
    max_retry: int = KFP_CLI_DEFAULT_RETRY,
) -> ApiRun:
    """Trigger KFP flow by pipeline name. See run_kubeflow_pipeline_by_id for more details."""
    client: KFPClient = _get_kfp_client()
    kubeflow_pipeline_id: str = client.get_pipeline_id(name=pipeline_name)
    return run_kubeflow_pipeline_by_id(
        pipeline_id=kubeflow_pipeline_id,
        kubeflow_namespace=kubeflow_namespace,
        triggered_run_name=triggered_run_name,
        experiment_name=kubeflow_experiment_name,
        pipeline_version_id=pipeline_version_id,
        parameters=parameters,
        max_retry=max_retry,
    )


def run_kubeflow_pipeline_by_id(
    pipeline_id: str,
    kubeflow_namespace: str,
    triggered_run_name: str,
    experiment_name: Optional[str] = None,
    pipeline_version_id: Optional[str] = None,
    parameters: Optional[dict] = None,
    max_retry: int = KFP_CLI_DEFAULT_RETRY,
) -> ApiRun:
    """Trigger KFP flow by pipeline id.

    pipeline_id: Pipeline id for which a new run should be triggered.
    experiment_name: Experiment where the triggered run should be placed.
    job_name: Job name of the flow being triggered.

    Return run id of created run. To reconstruct url see run_id_to_url function.
    """

    def create_experiment():
        return client.create_experiment(
            name=experiment_name,
            description="Experiment flow trigger from same namespace",
            namespace=kubeflow_namespace,
        )

    def run_pipeline():
        return client.run_pipeline(
            experiment_id=experiment.id,
            job_name=triggered_run_name,
            pipeline_id=pipeline_id,
            params={"flow_parameters_json": json.dumps(parameters)},
            version_id=pipeline_version_id,
        )

    if parameters is None:
        parameters = {}

    client = _get_kfp_client()
    experiment: ApiExperiment = _retry(create_experiment, max_retry)
    pipeline_run: ApiRun = _retry(run_pipeline, max_retry)
    logger.info(f"Triggered run {pipeline_run.id} - {run_id_to_url(pipeline_run.id)}")
    return pipeline_run


def run_id_to_url(run_id: str):
    return posixpath.join(
        KFP_RUN_URL_PREFIX,
        "_/pipeline/#/runs/details",
        run_id,
    )


def is_finished_run(api_run: ApiRun):
    run_status = api_run.status
    return run_status and run_status.lower() in [
        "succeeded",
        "failed",
        "skipped",
        "error",
    ]


def get_kfp_run(run_id, retry=KFP_CLI_DEFAULT_RETRY, client: KFPClient = None):
    client: KFPClient = client or _get_kfp_client()
    return _retry(client.get_run, max_attempt=retry, run_id=run_id).run


def wait_for_kfp_run_completion(
    run_id: str,
    wait_timeout: [int, datetime.timedelta] = 0,
    min_check_delay: int = 5,
    max_check_delay: int = 30,
    retry: int = KFP_CLI_DEFAULT_RETRY,
) -> ApiRun:
    """Check for KFP run status.

    If timeout (in second) is positive this function waits for flow to complete.
    Raise timeout if run is not finished after <timeout> seconds
    - finished or not (bool)
    - success or not (bool)
    - run info (kfp_server_api.ApiRun)

    Status check frequency will be close to min_check_delay for the first 11 minutes,
    and gradually approaches max_check_delay after 23 minutes.

    A close mimic to async is to use get_kfp_run above.
    Implementation for async is not prioritized until specifically requested.

    TODO(yunw)(AIP-5671): Async version
    """

    def get_delay(secs_since_start, min_delay, max_delay):
        """
        this sigmoid function reaches
        - 0.1 after 11 minutes
        - 0.5 after 15 minutes
        - 1.0 after 23 minutes
        in other words, the the delay is close to min_delay during the first 10 minutes
        """
        sigmoid = 1.0 / (1.0 + math.exp(-0.01 * secs_since_start + 9.0))
        return min_delay + sigmoid * (max_delay - min_delay)

    client: KFPClient = _get_kfp_client()
    run: ApiRun = get_kfp_run(run_id=run_id, retry=retry, client=client)

    if not is_finished_run(run) != "succeeded" and wait_timeout:
        if isinstance(wait_timeout, datetime.timedelta):
            wait_timeout = wait_timeout.total_seconds()

        # A mimic of kfp.Client.wait_for_run_completion with customized logging
        logger.info(
            f"Waiting for run {run_id} to finish. Timeout: {wait_timeout} second(s)"
        )
        start_time = datetime.datetime.now()
        while not is_finished_run(run):
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.info(
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
    logging.info(f"Terminating run {run_id}")
    run_service_api = RunServiceApi()
    return _retry(run_service_api.terminate_run, retry, run_id=run_id, **kwargs)
