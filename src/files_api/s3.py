import boto3
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef

BUCKET_NAME = "cloud-course-bucket-rhett"

session = boto3.Session(profile_name="cloud-course")
s3_client: S3Client = session.client("s3")

# write a file to s3 bucket with the contents "Hello, World!"
response: PutObjectOutputTypeDef = s3_client.put_object(
    Bucket=BUCKET_NAME, Key="folder/hello.txt", Body="Hello, World!", ContentType="text/plain"
)
