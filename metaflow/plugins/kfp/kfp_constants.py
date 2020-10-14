# Constants used in run MF flow on KFP

# Defaults for running MF on KFP
DEFAULT_KFP_YAML_OUTPUT_PATH = "kfp_pipeline.yaml"
DEFAULT_RUN_NAME = "run_mf_on_kfp"
DEFAULT_EXPERIMENT_NAME = "mf-on-kfp-experiments"

BASE_IMAGE = "hsezhiyan/metaflow-zillow:1.1"

KFP_METAFLOW_CONTEXT_DICT_PATH = "/tmp/kfp_metaflow_context_dict.json"

SPLIT_SEPARATOR = "_"
PASSED_IN_SPLIT_INDEXES_ENV_NAME = "PASSED_IN_SPLIT_INDEXES_ENV_NAME"
