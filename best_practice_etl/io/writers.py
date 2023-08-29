from typing import List

from pyspark.sql import DataFrame


class S3Writer:
    def __init__(self, bucket: str, path: str):
        self.bucket = bucket
        self.path = path

    def write_json(self, df: DataFrame, partition_by: List[str] = None):
        if partition_by is None:
            partition_by = []

        path = f"s3://{self.bucket}/{self.path}"

        df.write.mode("overwrite").json(path=path)

    def write_parquet(self, df: DataFrame, partition_by: List[str] = None):
        if partition_by is None:
            partition_by = []

        path = f"s3://{self.bucket}/{self.path}"

        df.coalesce(1) \
            .write \
            .option("partitionOverwriteMode", "dynamic") \
            .partitionBy(*partition_by) \
            .parquet(path, compression='snappy', mode="overwrite")

