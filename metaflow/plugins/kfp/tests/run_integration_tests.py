import argparse
from os import listdir
from os.path import isfile, join
from subprocess import run, PIPE

import kfp

from metaflow.util import get_username
from metaflow.metaflow_config import KFP_SDK_API_NAMESPACE, KFP_RUN_URL_PREFIX

import pytest

"""
From the root of this project, run: 
`python -m pytest -s -n 3 metaflow/plugins/kfp/tests/run_integration_tests.py`

This script runs all the flows in the `sample_flows` directory. It creates
each kfp run, waits for the run to fully complete, and prints whether
or not the run was successful. It also checks to make sure the logging
functionality works.

More specifically, the tests spawn KFP runs, and ensure the spawning processes
returns a returncode of 0. However, the logs of the run within KFP are simply logged
to the screen, and it is the responsibility of the user to read these logs to ensure
the run executed correctly on KFP.

Parameters:
-n: specifies the number of parallel processes used by PyTest.

"""


def parse_run_id(output):
	start_idx = output.find("run_id|")
	end_idx = output.find("|end_id")

	if start_idx == -1 or end_idx == -1:
		return -1

	run_id = output[start_idx + len("run_id|"):end_idx]
	return run_id


def obtain_flow_file_paths(flow_dir_path):
    file_paths = [f for f in listdir(flow_dir_path) if isfile(join(flow_dir_path, f))]
    return file_paths


@pytest.mark.parametrize(
    'flow_file_path',
    obtain_flow_file_paths("metaflow/plugins/kfp/tests/sample_flows")
)
def test_sample_flows(flow_file_path):
    full_path = join("metaflow/plugins/kfp/tests/sample_flows", flow_file_path)
    run_and_wait_process = run(
        f"python3 {full_path} --datastore=s3 kfp run --wait-for-completion",
        text=True,
        stdout=PIPE,
        shell=True
    )   
    assert run_and_wait_process.returncode == 0
    
    # we check for the correct logging of only the 'start' and 'end'
    # steps because these are the only steps gauranteed by Metaflow
    run_id = parse_run_id(run_and_wait_process.stdout)
    check_valid_logs_process = run(
        f"python3 {full_path} --datastore=s3 logs kfp-{run_id}/start &&"
        f"python3 {full_path} --datastore=s3 logs kfp-{run_id}/end",
        text=True,
        shell=True
    )
    assert check_valid_logs_process.returncode == 0

    return
