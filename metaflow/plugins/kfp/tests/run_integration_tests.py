from os import listdir
from os.path import isfile, join
from subprocess import run, PIPE
from typing import List

from .... import R

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

Sometimes, the tests may fail on KFP due to resource quota issues. If they do,
try reducing -n (number of parallel processes) so less simulateneous
KFP runs will be scheduled.

"""


def _python():
    if R.use_r():
        return "python3"
    else:
        return "python"


def obtain_flow_file_paths(flow_dir_path: str) -> List[str]:
    file_paths = [
        file_name
        for file_name in listdir(flow_dir_path)
        if isfile(join(flow_dir_path, file_name)) and not file_name.startswith(".")
    ]
    return file_paths


@pytest.mark.parametrize("flow_file_path", obtain_flow_file_paths("sample_flows"))
def test_sample_flows(pytestconfig, flow_file_path: str) -> None:
    full_path = join("sample_flows", flow_file_path)
    # In the process below, stdout=PIPE because we only want to capture stdout.
    # The reason is that the click echo function prints to stderr, and contains
    # the main logs (run link, graph validation, package uploading, etc). We
    # want to ensure these logs are visible to users and not captured.
    # We use the print function in kfp_cli.py to print a magic token containing the
    # run id and capture this to correctly test logging. See the
    # `check_valid_logs_process` process.
    run_and_wait_process = run(
        f"{_python()} {full_path} --datastore=s3 kfp run --no-s3-code-package"
        f" --wait-for-completion --base-image {pytestconfig.getoption('tag')}",
        universal_newlines=True,
        stdout=PIPE,
        shell=True,
    )
    assert run_and_wait_process.returncode == 0

    return
