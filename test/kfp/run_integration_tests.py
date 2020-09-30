from metaflow.plugins.kfp.kfp_cli import run
from metaflow import FlowSpec, step
from test.kfp.sample_flows import *

from os import listdir
from os.path import isfile, join

from io import StringIO, BytesIO
import sys
import subprocess
from subprocess import Popen, PIPE, STDOUT

import kfp

# x = linear_flow_1.LinearFlow1
# print(x)

# test_suite = [
#     linear_flow_1.LinearFlow1
# ]

def parse_run_id(output):
    run_id_flag = False
    run_id = ""
    for i, char in enumerate(output):
        if i >= 7 and output[i-7:i] == "run_id:":
            run_id_flag = True
        if run_id_flag and char == '\n':
            return run_id
        if run_id_flag:
            run_id += char
    return -1

def obtain_flow_file_paths(dir_path):
    file_paths = [f for f in listdir(dir_path) if isfile(join(dir_path, f))]
    file_paths.remove("__init__.py")
    return file_paths

def test_sample_flows(dir_path="./test/kfp/sample_flows"):
    flow_file_paths = obtain_flow_file_paths(dir_path)
    kfp_client = kfp.Client(namespace="kubeflow", userid="hariharans@zillowgroup.com") 
    for flow_file_path in flow_file_paths:
        full_path = join(dir_path, flow_file_path)
        process = subprocess.run(
            ["python", full_path, "--datastore=s3", "kfp", "run"],
            stdout=PIPE, stderr=PIPE,
            check=True,
            text=True
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
    # subprocess.call(["python", "./test/kfp/sample_flows/linear_flow_1.py", "--datastore=s3", "kfp", "run"])
    # test_sample_flow("./test/kfp/sample_flows/linear_flow_1.py")
    test_sample_flows()