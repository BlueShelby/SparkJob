from src.io.readers import S3Reader
from src.io.writers import S3Writer
from src.jobs.constants import RESOURCE_TYPES
from pyspark.sql.types import StructType

from src.transforms.transformation_factory import TransformationFactory
from src.jobs.base_spark_job import BaseSparkJob
from pyspark.sql import functions as F


class ProcessJob(BaseSparkJob):
    def __init__(self, session_name: str, reader: S3Reader, writer: S3Writer):
        self.reader = reader
        self.writer = writer
        super().__init__(session_name)

    def execute(self, organization_id=None, account_id=None, snapshot_id=None, assessment_id=None):
        df = self.reader.read_parquet(self.spark)

        transformation_factory = TransformationFactory(self.spark)

        # Empty df
        result = self.spark.createDataFrame([], schema=StructType([]))
        result = result.withColumn("key_index", F.lit(1))

        for resource_type, resource_key in RESOURCE_TYPES.items():
            transformation = transformation_factory.get_transformation(resource_type)
            if transformation:
                print(f"Processing resource: {resource_type}")
                transformed_df = transformation.apply(df)
                if transformed_df is not None:
                    transformed_df = transformed_df.withColumn("key_index", F.lit(1))
                    result = result.join(transformed_df, "key_index", "right")

        result = result.drop("key_index")
        result.show(truncate=True)

        self.writer.write_json(result)


