import os
import subprocess
from unittest import mock

from metaflow.mflog import save_logs
from metaflow.plugins.aip.aip import KubeflowPipelines
from metaflow.plugins.aip.aip_metaflow_step import _command


def test_set_pod_fs_group():
    workflow = {"spec": {"templates": []}}
    KubeflowPipelines._set_pod_fs_group(workflow)
    assert workflow["spec"]["securityContext"]["fsGroup"] == 100


def _run_volume_cleanup(volume_dir):
    cmd = _command(
        volume_dir=str(volume_dir),
        step_cli="true",
        task_id="t1",
        passed_in_split_indexes="",
        step_name="start",
        flow_name="sample_flow",
        is_interruptible=False,
    )
    # Format like aip_metaflow_step() does, so stray braces in the command would fail here.
    cmd = cmd.format(run_id="r1", passed_in_split_indexes="")
    clean_volume_cmd = cmd.split(" && ")[0]
    subprocess.run(clean_volume_cmd, shell=True, check=True)


def test_volume_cleanup_skips_lost_found(tmp_path):
    (tmp_path / "lost+found").mkdir()
    (tmp_path / "old_dir").mkdir()
    (tmp_path / "old_file").touch()

    _run_volume_cleanup(tmp_path)

    assert [p.name for p in tmp_path.iterdir()] == ["lost+found"]


def test_volume_cleanup_missing_dir_is_noop(tmp_path):
    _run_volume_cleanup(tmp_path / "does_not_exist")  # must not fail


def test_save_logs_without_mflog_env_is_noop():
    env = {k: v for k, v in os.environ.items() if not k.startswith("MF")}
    with mock.patch.dict(os.environ, env, clear=True):
        save_logs.save_logs()  # must not raise KeyError: 'MF_PATHSPEC'
