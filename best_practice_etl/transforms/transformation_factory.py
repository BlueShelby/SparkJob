from typing import Dict, Type

from best_practice_etl.transforms.instance_transformation import InstanceTransformation
from best_practice_etl.transforms.resource_transformation import ResourceTransformation
from best_practice_etl.transforms.s3Bucket_transformation import S3BucketTransformation
from pyspark.sql import SparkSession


class TransformationFactory:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.available_transformations: Dict[str, Type[ResourceTransformation]] = {
            "s3Buckets": S3BucketTransformation,
            "instances": InstanceTransformation
        }

    def get_transformation(self, resource_type: str) -> ResourceTransformation:
        if resource_type in self.available_transformations:
            transformation_class = self.available_transformations[resource_type]
            return transformation_class(self.spark)
        else:
            print(f'not available transformation: {resource_type}')
