from os import listdir
from os.path import isfile, join
from subprocess import run, PIPE

import kfp

import pytest

"""
From the folder `tests`, run: 
`python -m pytest -s -n 3 run_integration_tests.py`

This script runs all the flows in the `sample_flows` directory. It creates
each kfp run, waits for the run to fully complete, and prints whether
or not the run was successful. It also checks to make sure the logging
functionality works.

More specifically, the tests spawn KFP runs and ensure the spawning processes
have a returncode of 0. However, the logs of the runs within KFP are simply logged
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

    run_id = output[start_idx + len("run_id|") : end_idx]
    return run_id


def obtain_flow_file_paths(flow_dir_path):
    file_paths = [f for f in listdir(flow_dir_path) if isfile(join(flow_dir_path, f))]
    return file_paths


@pytest.mark.parametrize("flow_file_path", obtain_flow_file_paths("sample_flows"))
def test_sample_flows(flow_file_path):
    full_path = join("sample_flows", flow_file_path)
    # In the process below, stdout=PIPE because we only want to capture stdout.
    # The reason is that the click echo function prints to stderr, and contains
    # the main logging (run link, graph validation, package uploading, etc). We
    # want to ensure this logging is visible to users and not captured.
    # We use the print function is kfp_cli.py to print a magic token containing the
    # run id, and cpature
    run_and_wait_process = run(
        f"python3 {full_path} --datastore=s3 kfp run --wait-for-completion",
        text=True,
        stdout=PIPE,
        shell=True,
    )
    assert run_and_wait_process.returncode == 0

    # we check for the correct logging of only the 'start' and 'end'
    # steps because these are the only steps gauranteed to exist
    # in a Metaflow flow
    run_id = parse_run_id(run_and_wait_process.stdout)
    assert run_id != -1
    check_valid_logs_process = run(
        f"python3 {full_path} --datastore=s3 logs kfp-{run_id}/start &&"
        f"python3 {full_path} --datastore=s3 logs kfp-{run_id}/end",
        text=True,
        shell=True,
    )
    assert check_valid_logs_process.returncode == 0

    return
