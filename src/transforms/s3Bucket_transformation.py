from src.transforms.resource_transformation import ResourceTransformation
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class S3BucketTransformation(ResourceTransformation):
    @property
    def resource_type(self) -> str:
        return "s3Bucket"

    @property
    def resource_alias(self) -> str:
        return "buckets"

    @property
    def resource_key(self) -> str:
        return "aws-s3"

    def apply_custom_cols(self, df: DataFrame) -> DataFrame:

        return df.withColumn("json_detail",
                             F.when(F.col("json_detail.name").like("%log%"),
                                    F.struct(
                                           F.col("json_detail.*"),
                                           F.lit(True).alias("isLogFile")
                                       )).otherwise(
                                 F.struct(
                                       F.col("json_detail.*"),
                                       F.lit(False).alias("isLogFile")
                                   )
                               ))

    def apply(self, df: DataFrame) -> DataFrame | None:
        return super().apply(df)


