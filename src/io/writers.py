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

        try:
            df.coalesce(1).write.mode("overwrite").json(path="test/output")
        except Exception as e:
            print(f"An error occurred trying to write JSON result: {e}")
            raise e

