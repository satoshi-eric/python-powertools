
import boto3

from typing import Dict

from aws_lambda_powertools.utilities.streaming.s3_object import S3Object
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes.s3_event import S3Event
from aws_lambda_powertools.utilities.streaming.transformations import (CsvTransform)


def lambda_handler(event: Dict[str, str], context: LambdaContext):
    s3_event = S3Event(event)
    s3_object = S3Object(bucket=s3_event.bucket_name, key=s3_event.object_key)
    data = s3_object.transform([CsvTransform()])
    for line in data:
        print(line)