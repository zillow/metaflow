# Integration Tests for Metaflow on KFP

These integration tests are based on Metaflow flows specifically designed the catch hard-to-find bugs. They can be run locally, or on Gitlab.

# Local Tests

To run the tests locally, first create your Metaflow config file locally, which can be found at: `/Users/[your_username]/.metaflowconfig/config.json`.
This can also be obtained by running `metaflow configure show`. 

A sample configuration:
```
{
    "METAFLOW_DATASTORE_SYSROOT_S3": "s3://aip-example-dev/metaflow",
    "METAFLOW_DEFAULT_DATASTORE": "local",
    "KFP_RUN_URL_PREFIX": "https://kubeflow.corp.dev.zg-aip.net/",
    "KFP_SDK_NAMESPACE": "aip-example",
    "METAFLOW_USER": "hariharans@zillowgroup.com"
}
```

Then, within the `tests` directory, run `python -m pytest -s -n 3 run_integration_tests.py --local`. The parameter `-n` specifies the number of parallel tests. You'll likely need to change this if resource constraints are an issue.

# Github Tests

These tests are configured to automatically run whenever you push a commit to the Github repository. A mirrored Gitlab repository detects changes, pulls in the changes, and triggers a pipeline.

Currently, due to Gitlab's polling, it takes about 20 minutes for these tests to be triggered automatically. To trigger these tests manually, run:

`curl -X POST "https://gitlab.zgtools.net/api/v4/projects/20508/mirror/pull?private_token=[PRIVATE_TOKEN]"`

Please reach out to @hariharans on Slack (for Zillow employees) to obtains the private token to run on Zillow internal Gitlab infrastructure.