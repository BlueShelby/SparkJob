from abc import ABC, abstractmethod
from typing import Callable

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class ResourceTransformation(ABC):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _filter(self, id_col: str, value: any) -> Callable[[DataFrame], DataFrame]:
        def _func(df: DataFrame) -> DataFrame:
            return df.filter((F.col("resource_id").isNotNull()) & (F.col(id_col) == value))

        return _func

    def _add_json_column(self, schema: StructType) -> Callable[[DataFrame], DataFrame]:
        def _func(df: DataFrame) -> DataFrame:
            return df.withColumn("json_detail", F.from_json(F.col("provider_data"), schema))
        return _func

    def _group_by_region_resource(self, df: DataFrame) -> DataFrame:
        return df.groupBy("region").agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("resource_id"),
                        F.struct(
                            F.col("resource_id").alias("resourceId"),
                            F.col("resource_name").alias("resourceName"),
                            F.col("region").alias("region"),
                            F.col("service_type").alias("serviceType"),
                            F.struct(
                                F.col("json_detail.*")
                            ).alias("providerData")
                        )
                    )
                )
            ).alias(self.resource_alias)
        ).groupBy().agg(F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("region"),
                    F.struct(
                        F.col(self.resource_alias)
                    )
                )
            )
        ).alias("regions")).select(F.struct(F.col("regions")).alias(self.resource_key))

    @property
    @abstractmethod
    def resource_type(self) -> str:
        pass

    @property
    @abstractmethod
    def resource_alias(self) -> str:
        pass

    @property
    @abstractmethod
    def resource_key(self) -> str:
        pass

    @abstractmethod
    def apply_custom_cols(self, df: DataFrame) -> DataFrame:
        return df

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame | None:

        filtered_rows = df.filter(F.col("resource_type") == self.resource_type).select("provider_data").take(1)

        if filtered_rows and filtered_rows[0][0]:
            print("applying base transformation.")
            json_schema = F.schema_of_json(filtered_rows[0][0])
            return df \
                .filter((F.col("resource_id").isNotNull())) \
                .transform(self._filter("resource_type", self.resource_type)) \
                .transform(self._add_json_column(json_schema)) \
                .transform(self.apply_custom_cols) \
                .transform(self._group_by_region_resource)

        else:
            return None






