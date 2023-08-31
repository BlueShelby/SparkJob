from pyspark.sql import DataFrame, SparkSession


class S3Reader:
    def __init__(self, bucket: str, path: str):
        self.bucket = bucket
        self.path = path

    def read_json(self, spark: SparkSession, path: str) -> DataFrame:
        raise NotImplementedError()

    def read_parquet(self, spark: SparkSession) -> DataFrame:
        path = f"s3://{self.bucket}/{self.path}"

        try:
            return spark.read.parquet("test/input")
        except Exception as e:
            print(f"An error occurred trying to read PARQUET result: {e}")
            raise e

