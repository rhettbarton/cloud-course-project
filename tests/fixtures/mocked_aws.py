import os

import boto3
from moto import mock_aws
from pytest import fixture

from tests.consts import TEST_BUCKET_NAME


def point_away_from_aws():
    # This is a function that does not interact with AWS.
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    return 42


@fixture
def mocked_aws() -> None:
    with mock_aws():
        # point away from AWS (to be extra careful)
        point_away_from_aws()

        # create an s3 bucket
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)

        yield

        # clean up by deleting the s3 bucket
        response = s3_client.list_objects_v2(Bucket=TEST_BUCKET_NAME)
        for obj in response.get("Contents", []):
            s3_client.delete_object(Bucket=TEST_BUCKET_NAME, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=TEST_BUCKET_NAME)
