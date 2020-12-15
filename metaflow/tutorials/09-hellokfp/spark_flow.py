from metaflow import FlowSpec, step, resources, spark


class SparkFlow(FlowSpec):
    """
    A flow which uses the @spark decorator to run a spark job within a single step.
    """

    @spark()
    @step
    def start(self):
        from pyspark.sql import SparkSession
        sample_text = "aaaaabbbbbccccc\naaaaabbbbbccccc\naaaaabbbbbccccc"
        sample_text_file = "/home/zservice/sample_text.txt"
        sample_text_file_f = open(sample_text_file, "w")
        sample_text_file_f.write(sample_text)
        sample_text_file_f.close()

        spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
        logData = spark.read.text(sample_text_file).cache()

        numAs = logData.filter(logData.value.contains('a')).count()
        numBs = logData.filter(logData.value.contains('b')).count()

        print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

        spark.stop()

        self.numAs = str(numAs)
        self.numBs = str(numBs)
        self.next(self.end)
    
    @step
    def end(self):
        print("numAs: ", self.numAs)
        print("numBs: ", self.numBs)

        assert self.numAs == "3"
        assert self.numBs == "3"

        print("___ENDING SPARK PIPELINE____")

if __name__ == '__main__':
    SparkFlow()
