from abc import abstractmethod

from pyspark.sql import SparkSession


class BaseSparkJob:

    def __init__(self, session_name: str):
        self.spark: SparkSession = self.create_spark_session(session_name)
        self.spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    @abstractmethod
    def execute(self):
        pass

    @staticmethod
    def create_spark_session(app_name: str) -> SparkSession:
        return (
            SparkSession
            .builder
            .appName(app_name)
            .enableHiveSupport()
            .config('spark.sql.caseSensitive', 'true')
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=UTC')
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=UTC')
            .config('spark.sql.session.timeZone', 'UTC')
            .config("spark.driver.memory", "8g")
            .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
            .config("spark.sql.maxToStringFields", 1000)
            .getOrCreate()
        )
