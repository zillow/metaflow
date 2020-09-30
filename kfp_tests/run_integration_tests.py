import argparse
from os import listdir
from os.path import isfile, join
from subprocess import run, PIPE

import kfp

from metaflow.util import get_username
from metaflow.metaflow_config import KFP_SDK_API_NAMESPACE


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


def parse_run_id(output):
    run_id_flag = False
    run_id = ""
    for i, char in enumerate(output):
        if i >= 7 and output[i - 7 : i] == "run_id:":
            run_id_flag = True
        if run_id_flag and char == "\n":
            return run_id
        if run_id_flag:
            run_id += char
    return -1


def obtain_flow_file_paths(flow_dir_path):
    file_paths = [f for f in listdir(flow_dir_path) if isfile(join(flow_dir_path, f))]
    file_paths.remove("__init__.py")  # __init__.py needed to establish Python module
    return file_paths


def test_sample_flows(args):
    flow_file_paths = obtain_flow_file_paths(args.flow_dir_path)
    kfp_client = kfp.Client(namespace=args.namespace, userid=args.userid)
    for flow_file_path in flow_file_paths:
        full_path = join(args.flow_dir_path, flow_file_path)
        print(f"Running {flow_file_path} ...")
        process = run(
            ["python", full_path, "--datastore=s3", "kfp", "run"],
            stdout=PIPE,
            stderr=PIPE,
            check=True,
            text=True,
        )
        run_id = parse_run_id(process.stdout)
        if run_id != -1:
            run_response = kfp_client.wait_for_run_completion(run_id, 500).to_dict()
            if run_response["run"]["status"] == "Succeeded":
                print(f"Flow {flow_file_path} ran successfully!")
            else:
                print(f"Flow {flow_file_path} encountered a runtime error.")
        else:
            print(f"Flow {flow_file_path} failed to compile properly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow_dir_path", type=str, default="./kfp_tests/sample_flows")
    parser.add_argument("--namespace", type=str, default=KFP_SDK_API_NAMESPACE)
    parser.add_argument("--userid", type=str, default=get_username())
    args = parser.parse_args()
    test_sample_flows(args)
