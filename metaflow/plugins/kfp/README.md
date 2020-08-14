### Commands used and References

###### Last Updated: Aug 12th:

#### Steps to run Metaflow on Kubeflow: 

##### Option 1:
1. Configure a metaflow profile. To do this, create a file named as `config_<your-metaflow-profile-name>.json` under 
`~/.metaflowconfig/`) and set the required values. Required fields to run on KFP are shared below*.
2. Create your runs on KFP using the following command template: 
```
export METAFLOW_PROFILE=<your-metaflow-profile-name> && 
python <program-name.py> [run-on-kfp|generate-kfp-yaml] (--code-url <link-to-code>)
``` 

##### Option 2:
You can export the required variables* individually and run the python command:

*Required keys in `config_<profile-name>.json` to run on KFP are mentioned below:

```
{
    "METAFLOW_DATASTORE_SYSROOT_S3": "s3://<path-to-s3-bucket-root>",
    "METAFLOW_DEFAULT_DATASTORE": "s3",

    "METAFLOW_AWS_ARN" : "...", # required to be able to read s3 data when running within KFP
    "METAFLOW_AWS_S3_REGION" : "...", # specify s3 region being used

    "KFP_RUN_URL_PREFIX" : "https://kubeflow.corp.zillow-analytics-dev.zg-int.net/pipeline/#/runs/details" # prefix of the URL preceeding the run-id to generate correct links to the generated runs on your cluster
}
```


##### Example using a METAFLOW_PROFILE named `sree` (option 1):

Config file: `config_sree.json` (to be saved under `~/.metaflowconfig`):
Contents of the config file:
```
{
    "METAFLOW_DATASTORE_SYSROOT_S3": "s3://workspace-zillow-analytics-stage/aip/metaflow",
    "METAFLOW_DEFAULT_DATASTORE": "s3",

    "METAFLOW_AWS_ARN" : "arn:aws:iam::170606514770:role/dev-zestimate-role",
    "METAFLOW_AWS_S3_REGION" : "us-west-2",

    "KFP_RUN_URL_PREFIX" : "https://kubeflow.corp.zillow-analytics-dev.zg-int.net/pipeline/#/runs/details"
}
```

To `run-on-kfp` using this profile:
```
export METAFLOW_PROFILE=sree && 
python 00-helloworld/hello.py run-on-kfp 
    --experiment-name "MF-on-KFP-P2" 
    --run-name "hello_run" 
    --code-url https://raw.githubusercontent.com/zillow/metaflow/mf-on-kfp-2/metaflow/tutorials/00-helloworld/hello.py
```

To `generate-kfp-yaml` using this profile:
```
export METAFLOW_PROFILE=sree && 
python 00-helloworld/hello.py generate-kfp-yaml
      --code-url https://raw.githubusercontent.com/zillow/metaflow/mf-on-kfp-2/metaflow/tutorials/00-helloworld/hello.py  
```


#####  Example of `run-on-kfp` without configuring a profile (option 2):
```
export METAFLOW_AWS_ARN="arn:aws:iam::170606514770:role/dev-zestimate-role" && 
export METAFLOW_AWS_S3_REGION="us-west-2" && 
export METAFLOW_DATASTORE_SYSROOT_S3="s3://workspace-zillow-analytics-stage/aip/metaflow" && 
export KFP_RUN_URL_PREFIX="https://kubeflow.corp.zillow-analytics-dev.zg-int.net/pipeline/#/runs/details/" && 
python 00-helloworld/hello.py run-on-kfp 
    --code-url="https://raw.githubusercontent.com/zillow/metaflow/state-integ-s3/metaflow/tutorials/00-helloworld/hello.py"
```

#### What's happening inside the step_container_op:

We execute the above local orchestration commands after performing the necessary setup. The current setup includes the following:

- Download the script to be run (needed as we aren't solving code packaging yet)
- Install the modified metaflow version (from Zillow's fork of Metaflow where we are pushing our changes)
- Set a KFP user
- Run the step command


#### Executing flow containing foreach locally using manual orchestration (Work in progress)

The foreach case is special as the number of splits at the end of a
node that defines a `foreach` transition is only known at runtime when the 
transition is encountered. 

During the execution of a metaflow step, when the `self.next` statement defining a `foreach` is encountered,
a private variable of the flow object (i.e., `_foreach_num_splits`) gets set which specifies the number of 
splits that result from the foreach. In other words this is the length of the iterable on which the foreach is
called. Knowing this value is key to defining the commands needed to execute the next steps.

For eg., in `foreach_flow.py`, the `start` step is a `foreach` node that 
invokes the `explore` node. To know the number of such invocations, we print out the value
of the `_foreach_num_splits` variable at the end of the start step. 

Now, based on this, we can invoke the `explore` step with the correct `--split-index` values ranging from
`0` to `_foreach_num_splits - 1`.

An example of foreach execution done by manual orchestration looks as follows 
(Note: Make sure to use different run_ids for each trial)

1. First, `init` is invoked to initialise the run
    ```
   python foreach_flow.py --datastore local init --run-id 1 --task-id 0
    ```
2. Then, we invoke the `start` step as follows:
    ```
   python foreach_flow.py --datastore local step start --run-id 1 --task-id 1 --input-paths 1/_parameters/0
    ```
   This step in `foreach_flow.py` is a `foreach` node and will print the value of `_foreach_num_splits` like below:
    ```
    foreach-numsplits:  4
   ```
   We can then use this value to invoke the next commands.
3. ```
   python foreach_flow.py --datastore local step explore 
                        --run-id 1 --task-id 2 
                        --input-paths 1/start/1 
                        --split-index 0 (use --split-index 1,2 and 3 to cover the remaining splits)
   ```
4. Then, we have a join. The `foreach join` works similar to the `branch join` but will need to be passed
a slightly different input path as we now have have multiple parent task-ids but from the same parent step.
So, the join command looks as follows:
    ```
    python foreach_flow.py --datastore local step join 
                        --run-id 1 --task-id 6 
                        --input-paths 1/explore/:2,3,4,5 (where 2,3,4,5 are the task_ids of the explore steps that were previously executed)
    ```
5. Finally, we have the end step which works the usual way as follows:
    ```
    python foreach_flow.py --datastore local step end 
                          --run-id 1 --task-id 9 
                          --input-paths 1/join/6   
    ```