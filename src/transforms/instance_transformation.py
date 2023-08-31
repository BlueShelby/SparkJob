from src.transforms.resource_transformation import ResourceTransformation
from pyspark.sql import DataFrame


class InstanceTransformation(ResourceTransformation):

    @property
    def resource_alias(self) -> str:
        return "instance"

    @property
    def resource_type(self) -> str:
        return "instance"

    @property
    def resource_key(self) -> str:
        return "aws-ec2"

    def apply_custom_cols(self, df: DataFrame) -> DataFrame:
        return super().apply_custom_cols(df)

    def apply(self, df: DataFrame) -> DataFrame | None:
        return super().apply(df)
