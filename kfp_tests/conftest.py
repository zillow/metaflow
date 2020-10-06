from metaflow.util import get_username
from metaflow.metaflow_config import KFP_SDK_API_NAMESPACE


def pytest_addoption(parser):
    parser.addoption("--flow_dir_path", type=str, default="./kfp_tests/sample_flows")
    parser.addoption("--namespace", type=str, default=KFP_SDK_API_NAMESPACE)
    parser.addoption("--userid", type=str, default=get_username())
