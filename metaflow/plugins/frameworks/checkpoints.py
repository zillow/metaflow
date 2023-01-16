import logging
import os
from typing import List, NamedTuple, Optional, TypeVar, Union
from urllib.parse import urlparse

from metaflow import S3, FlowSpec, Run, current

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler())
_logger.setLevel(logging.INFO)

T = TypeVar("T")


def _get_s3_latest_checkpoint_s3_info(
    s3_checkpoint_root: str,
):
    s3 = S3(s3root=s3_checkpoint_root)
    checkpoints = s3.list_paths([""])
    if not checkpoints:
        return None

    with_infos: List = s3.info_many([cpt.key for cpt in checkpoints])
    return max(with_infos, key=lambda info: info.last_modified)


def _get_latest_checkpoint_name(root: str) -> str:
    if is_path_s3(root):
        latest = _get_s3_latest_checkpoint_s3_info(root)
        return latest.key if latest else None
    else:
        files = os.listdir(root)
        paths = [os.path.join(root, basename) for basename in files]
        paths = [path for path in paths if os.path.isfile(path)]
        return max(paths, key=os.path.getctime)


def is_path_s3(path: str) -> bool:
    from metaflow.util import to_unicode

    parsed = urlparse(to_unicode(path))
    return parsed.scheme == "s3"


def _get_checkpoint_root(
    run: Optional[Union[FlowSpec, Run]] = None,
    step_name: Optional[str] = None,
) -> str:
    root_env = os.environ.get("CHECKPOINT_ROOT")
    if root_env:
        return root_env
    else:
        temp_step_name = step_name if step_name else current.step_name
        temp_run = run if run else current.flow
        return os.path.join(
            S3(run=temp_run)._s3root, f"checkpoints/{temp_step_name}/{current.task_id}"
        )


def _get_resume_checkpoint_path(
    root: str,
    resume_checkpoint_path: Optional[str] = None,
) -> Optional[str]:
    if resume_checkpoint_path and current.retry_count == 0:
        return resume_checkpoint_path

    if current.retry_count > 0:
        key = _get_latest_checkpoint_name(root)
        if key:
            ret = os.path.join(root, key)
            _logger.debug(f"_get_resume_checkpoint_path returning {ret}")
            return ret
        else:
            _logger.info(
                f"{current.retry_count=} but using {resume_checkpoint_path=} because no checkpiont found."
            )
            return resume_checkpoint_path
    else:
        return None


CheckpointPaths = NamedTuple(
    "CheckpointPaths", [("root", str), ("resume_path", Optional[str])]
)


def get_checkpoint_paths(
    resume_checkpoint_path: Optional[str] = None,
    run: Optional[Union[FlowSpec, Run]] = None,
    step_name: Optional[str] = None,
) -> CheckpointPaths:
    """
    This function gets the checkpoint root and resume path for a Flow Run step.

    The environment variable CHECKPOINT_ROOT is an override for the root path,
    which is useful for local or CICD runs not on S3.

    Args:
        resume_checkpoint_path (Optional[str], optional):
            If provided, this would be the initial CheckpointPaths.resume_path
            returned upon the first attempt, and upon retries.
            Defaults to None, upon which it returns None on the first attempt.
        run (Optional[Union[FlowSpec, Run]], optional):
            Either a FlowSpec object (typically 'self') or a Run object
            corresponding to an existing Metaflow run. These are used to add a
            version suffix in the S3 path.
            Defaults to None upon which it uses the current `current.flow`.
        step_name (Optional[str], optional):
            The step_name to resume from.
            Defaults to None, upon which it uses `current.step_name`.

    Returns:
        CheckpointPaths with folllowing:
        - root: Checkpoint root S3 path for this run and step.
        - resume_path: S3 path to the latest checkpoint under the root, to resume from.

    Examples:
        1. The resume_path is None on the first attempt::

            @interruptible
            @retry
            @step
            def train(self):
                checkpoint_root, resume_path = get_checkpoint_paths()
                ...

        2. `resume_path` is the latest file in the path on a second attempt:
        (s3://.../checkpoints/step_name/task_id/, s3://.../checkpoints/step_name/task_id/checkpoint1.pt)

        3. Example of resuming a checkpoint from a previous training run to resume training from.
        Any interruptions or retries would return the latest checkpoint path in `resume_path`::

            @interruptible
            @retry
            @step
            def train(self):
                checkpoint_root, resume_path = get_checkpoint_paths()
                ...

        (s3://.../checkpoints/step_name/task_id/, s3://.../previous_run_checkpoint_path.pt)
    """
    root: str = _get_checkpoint_root(run, step_name)
    return CheckpointPaths(
        root=root,
        resume_path=_get_resume_checkpoint_path(root, resume_checkpoint_path),
    )
