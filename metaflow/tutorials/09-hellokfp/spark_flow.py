from typing import List, Tuple
from metaflow import FlowSpec, step, resources, spark, Parameter
from metaflow.plugins.kfp.kfp import SparkSessionCreator

class SparkFlow(FlowSpec):
    """
    A flow which uses the @spark decorator to run a spark job within a single step.
    """
    spark_eventLog_enabled = Parameter(
        'spark_eventLog_enabled',
        help='param with default',
        default="false",
    )

    @spark()
    @step
    def start(self):
        sample_text = "aaaaabbbbbccccc\naaaaabbbbbccccc"
        sample_text_file = "/home/zservice/sample_text.txt"
        # sample_text_file = "/Users/hariharans/Desktop/sample_text.txt"

        with open(sample_text_file, "w") as sample_text_file_f:
            sample_text_file_f.write(sample_text)

        with SparkSessionCreator("SimpleSparkApp", [("spark.eventLog.enabled", self.spark_eventLog_enabled)]) as spark_sess:
            logData = spark_sess.read.text(sample_text_file).cache()

            numAs = logData.filter(logData.value.contains('a')).count()
            numBs = logData.filter(logData.value.contains('b')).count()

            print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
            
        self.numAs = str(numAs)
        self.numBs = str(numBs)
        self.next(self.end)
    
    @step
    def end(self):
        assert self.numAs == "2"
        assert self.numBs == "2"

        print("____ENDING SPARK PIPELINE____")

if __name__ == '__main__':
    SparkFlow()
