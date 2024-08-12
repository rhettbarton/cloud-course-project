"""Test cases for `s3.read_objects`."""

import os
from unittest import mock

import boto3
from moto import mock_aws

from files_api.s3.read_objects import (
    fetch_s3_objects_metadata,
    fetch_s3_objects_using_page_token,
    object_exists_in_s3,
)
from tests.consts import TEST_BUCKET_NAME


@mock_aws
def test_object_exists_in_s3(mocked_aws: None):
    """Test that the function correctly identifies whether an object exists in an S3 bucket."""
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=TEST_BUCKET_NAME, Key="test.txt")
    assert object_exists_in_s3(bucket_name=TEST_BUCKET_NAME, object_key="test.txt") is True
    assert object_exists_in_s3(bucket_name=TEST_BUCKET_NAME, object_key="nothere.txt") is False


@mock_aws
def test_pagination(mocked_aws: None):
    """Test that the function correctly paginates through the S3 objects."""
    s3_client = boto3.client("s3")
    for i in range(10):
        s3_client.put_object(Bucket=TEST_BUCKET_NAME, Key=f"test{i}.txt")

    # Test pagination for fetch_s3_objects_metadata
    objects, next_continuation_token = fetch_s3_objects_metadata(bucket_name=TEST_BUCKET_NAME, max_keys=5)
    assert len(objects) == 5
    assert objects[0]["Key"] == "test0.txt"
    assert objects[1]["Key"] == "test1.txt"
    assert objects[2]["Key"] == "test2.txt"
    assert objects[3]["Key"] == "test3.txt"
    assert objects[4]["Key"] == "test4.txt"
    assert next_continuation_token is not None

    # Test pagination for fetch_s3_objects_using_page_token
    objects, next_continuation_token = fetch_s3_objects_using_page_token(
        bucket_name=TEST_BUCKET_NAME, continuation_token=next_continuation_token, max_keys=3
    )
    assert len(objects) == 3
    assert objects[0]["Key"] == "test5.txt"
    assert objects[1]["Key"] == "test6.txt"
    assert objects[2]["Key"] == "test7.txt"
    assert next_continuation_token is not None

    objects, next_continuation_token = fetch_s3_objects_using_page_token(
        bucket_name=TEST_BUCKET_NAME, continuation_token=next_continuation_token, max_keys=3
    )
    assert len(objects) == 2
    assert objects[0]["Key"] == "test8.txt"
    assert objects[1]["Key"] == "test9.txt"
    assert next_continuation_token is None


@mock_aws
def test_directory_queries(mocked_aws: None):
    """Test that the function correctly fetches objects in a directory."""
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=TEST_BUCKET_NAME, Key="test/test/test1.txt")
    s3_client.put_object(Bucket=TEST_BUCKET_NAME, Key="test/test2.txt")
    s3_client.put_object(Bucket=TEST_BUCKET_NAME, Key="test3.txt")

    objects, next_continuation_token = fetch_s3_objects_metadata(bucket_name=TEST_BUCKET_NAME, prefix="test/")
    assert len(objects) == 2
    assert objects[0]["Key"] == "test/test/test1.txt"
    assert objects[1]["Key"] == "test/test2.txt"

    objects, next_continuation_token = fetch_s3_objects_metadata(bucket_name=TEST_BUCKET_NAME, prefix="test/test/")
    assert len(objects) == 1
    assert objects[0]["Key"] == "test/test/test1.txt"
