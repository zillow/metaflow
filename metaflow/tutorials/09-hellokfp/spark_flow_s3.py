from metaflow import FlowSpec, step, resources, spark, Parameter
from metaflow.plugins.kfp.kfp import SparkSessionCreator

import boto3

class AwsHelper:
    def __init__(self):
        session = boto3.Session()
        credentials = session.get_credentials()

        # Credentials are refreshable, so accessing your access key / secret key
        # separately can lead to a race condition.
        # Use this to get an actual matched set.
        credentials = credentials.get_frozen_credentials()
        self.access_key = credentials.access_key
        self.secret_key = credentials.secret_key
        self.token = credentials.token

    # use aws credential to access s3 in spark
    def authenticate_s3(self, spark_session, aws_region='us-west-2'):
        hadoop_config = spark_session.sparkContext._jsc.hadoopConfiguration()
        hadoop_config.set('fs.s3a.endpoint', f's3.{aws_region}.amazonaws.com')
        hadoop_config.set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
        hadoop_config.set('fs.s3a.access.key', self.access_key)
        hadoop_config.set('fs.s3a.secret.key', self.secret_key)
        hadoop_config.set('fs.s3a.session.token', self.token)

    # save spark DataFrame to s3 in csv format
    def save_to_s3_csv(self, spark_df, s3_path):
        spark_df.write.mode("Overwrite").format("csv").save(s3_path)

    # save spark DataFrame to s3 in parquet format
    def save_to_s3_parquet(self, spark_df, s3_path):
        spark_df.write.parquet(s3_path, mode="overwrite")


class SparkFlowS3(FlowSpec):
    """
    A flow which uses the @spark decorator to run a spark job within a single step.
    Additionally, this flow demonstrates that Spark applications on KFP have access to the same
    S3 credentials as the S3 credentials injected into a pod.
    """
    spark_eventLog_enabled = Parameter(
        'spark_eventLog_enabled',
        help='param with default',
        default="false",
    )

    @spark()
    @step
    def start(self):
        sample_text_file = "s3a://aip-example-dev/metaflow/SparkOnMetaflowData/sample_text_file.txt"

        configurations = [
            ("spark.eventLog.enabled", self.spark_eventLog_enabled),
            ('spark.driver.memory', '512m'),  
            ('spark.executor.memory', '512m')
        ]

        with SparkSessionCreator("SimpleSparkApp", configurations) as spark_sess:
            # setting up the IAM role for the Spark job
            aws_helper = AwsHelper()
            aws_helper.authenticate_s3(spark_sess)

            sc = spark_sess.sparkContext
            logData = sc.textFile(sample_text_file)

            counts = logData.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2: v1 + v2)
            df = counts.toDF().orderBy(["_2"], ascending=False)
            df.show(n=5)
            self.df_num_rows = df.count() # we can only persist serializable Python native variables across steps

            from datetime import datetime
            now = datetime.now()
            time_str = str(now.year) + str(now.month) + str(now.day) + str(now.hour) + str(now.minute) + str(now.second) + str(now.microsecond) 

            aws_helper.save_to_s3_csv(df, f"s3a://aip-example-dev/metaflow/SparkOnMetaflowData/sample_csv_files/{time_str}")
            # aws_helper.save_to_s3_parquet(df, f"s3a://aip-example-dev/metaflow/SparkOnMetaflowData/sample_parquet_files/{time_str}")

        self.next(self.end)
    
    @step
    def end(self):
        assert self.df_num_rows == 8
        print("___ENDING SPARK PIPELINE____")

if __name__ == '__main__':
    SparkFlowS3()
