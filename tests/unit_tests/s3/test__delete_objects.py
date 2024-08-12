"""Test cases for `s3.delete_objects`."""

import boto3
from moto import mock_aws

from files_api.s3.delete_objects import delete_s3_object
from files_api.s3.read_objects import object_exists_in_s3
from files_api.s3.write_objects import upload_s3_object
from tests.consts import TEST_BUCKET_NAME


@mock_aws
def test_delete_existing_s3_object(mocked_aws: None):
    """Test that the function correctly deletes an object from an S3 bucket."""
    upload_s3_object(bucket_name=TEST_BUCKET_NAME, object_key="test.txt", file_content=b"hello world")
    assert object_exists_in_s3(bucket_name=TEST_BUCKET_NAME, object_key="test.txt") is True
    delete_s3_object(bucket_name=TEST_BUCKET_NAME, object_key="test.txt")
    assert object_exists_in_s3(bucket_name=TEST_BUCKET_NAME, object_key="test.txt") is False


@mock_aws
def test_delete_nonexistent_s3_object(mocked_aws: None):
    """Test that the function does not raise an error when deleting a nonexistent object."""
    assert object_exists_in_s3(bucket_name=TEST_BUCKET_NAME, object_key="nothere.txt") is False
    delete_s3_object(bucket_name=TEST_BUCKET_NAME, object_key="nothere.txt")
    assert object_exists_in_s3(bucket_name=TEST_BUCKET_NAME, object_key="nothere.txt") is False
