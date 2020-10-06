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
`python3 -m kfp_tests.run_integration_tests --dir_path [dir_path]
    --namespace [namespace] --userid [userid]`

This script runs all the flows in the `sample_flows` directory. It creates
each kfp run, waits for the run to fully complete on KFP, and prints whether
or not the run was successful. 

The 3 argparse arguments are optional and should not be needed, but are provided 
if you want to test for another namespace or from another user's account.

Arguments:
- flow_dir_path -- the directory where the sample flows are located. 
    This should only change if you're not running this script from 
    the root directory.
- namespace -- the namespace on KF where the runs are created.
    The default is obtained from your metaflow config file.
- userid -- your userid for KF. The default is also 
    obtained from your metaflow config file.
"""


@pytest.fixture()
def flow_dir_path(pytestconfig):
    return pytestconfig.getoption("flow_dir_path")


@pytest.fixture()
def namespace(pytestconfig):
    return pytestconfig.getoption("namespace")


@pytest.fixture()
def userid(pytestconfig):
    return pytestconfig.getoption("userid")


def obtain_flow_file_paths(flow_dir_path):
    file_paths = [f for f in listdir(flow_dir_path) if isfile(join(flow_dir_path, f))]
    return file_paths


def spawn_and_return(flow_dir_path, flow_file_path):
    """
    This function is the creation and waiting one KFP run. Since
    we don't want to wait for the entire flow to finish on KFP before
    creating a new run, we will have Ray parallelize the effort.
    """
    full_path = join(flow_dir_path, flow_file_path)
    process = run(
        ["python3", full_path, "--datastore=s3", "kfp", "run", "--wait-for-completion"],
        check=True,
        text=True,
    )
    return process.returncode


def test_sample_flows(flow_dir_path, namespace, userid):
    flow_file_paths = obtain_flow_file_paths(flow_dir_path)
    for (
        flow_file_path
    ) in flow_file_paths:  # we append tasks, which immediately return a "future"
        return_code = spawn_and_return(flow_dir_path, flow_file_path)
        assert return_code == 0
    return
