from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from generic_cleanup_demo.config.ConfigStore import *
from generic_cleanup_demo.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Generic_Cleanup_Demo")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Generic_Cleanup_Demo")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Generic_Cleanup_Demo", config = Config)(pipeline)

if __name__ == "__main__":
    main()
