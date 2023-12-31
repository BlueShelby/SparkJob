# tests/test_s3bucket_transformation.py
import json

import pytest

from src.transforms.s3Bucket_transformation import S3BucketTransformation
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session():
    spark = SparkSession.builder.appName("TestS3BucketTransformation").getOrCreate()
    yield spark
    spark.stop()


def test_s3bucket_transformation(spark_session):
    # Arrange
    spark = spark_session

    s3_transformation = S3BucketTransformation(spark)
    data = [
        ("eu-central-1", "aws", "s3Bucket", "arn:aws:s3:::help.spotinst.com_v1", "s3Bucket", "aws-s3", "{\"name\":\"help.spotinst.com_v1\",\"creationDate\":\"2020-03-25T18:03:49.000Z\"}"),
        ("eu-central-2", "aws", "s3Bucket", "arn:aws:s3:::help.spotinst.com_v2", "s3Bucket", "aws-s3", "{\"name\":\"help.spotinst.com_v2\",\"creationDate\":\"2020-03-25T18:03:49.000Z\"}"),
    ]
    columns = ["region", "cloud_provider", "resource_type", "resource_id", "resource_name", "service_type", "provider_data"]
    input_df = spark.createDataFrame(data, columns)

    expected_dict = {
        "aws-s3": {
            "regions": {
                "eu-central-1": {
                    "buckets": {
                        "arn:aws:s3:::help.spotinst.com_v1": {
                            "resourceId": "arn:aws:s3:::help.spotinst.com_v1",
                            "resourceName": "s3Bucket",
                            "region": "eu-central-1",
                            "serviceType": "aws-s3",
                            "providerData": {
                                "creationDate": "2020-03-25T18:03:49.000Z",
                                "name": "help.spotinst.com_v1",
                                "isLogFile": False
                            }
                        }
                    }
                },
                "eu-central-2": {
                    "buckets": {
                        "arn:aws:s3:::help.spotinst.com_v2": {
                            "resourceId": "arn:aws:s3:::help.spotinst.com_v2",
                            "resourceName": "s3Bucket",
                            "region": "eu-central-2",
                            "serviceType": "aws-s3",
                            "providerData": {
                                "creationDate": "2020-03-25T18:03:49.000Z",
                                "name": "help.spotinst.com_v2",
                                "isLogFile": False
                            }
                        }
                    }
                }
            }
        }
    }

    # Act
    result_df = s3_transformation.apply(input_df)

    # Asserts
    assert result_df.toJSON().collect() == [json.dumps(expected_dict, separators=(',', ':'))]
