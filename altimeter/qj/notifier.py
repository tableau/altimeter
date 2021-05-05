"""ResultSetNotifier - sends SNS messages when a result set is created"""
from dataclasses import dataclass

import boto3

from altimeter.qj import schemas


@dataclass(frozen=True)
class ResultSetNotifier:
    sns_topic_arn: str
    region_name: str

    def notify(self, notification: schemas.ResultSetNotification) -> None:
        session = boto3.Session(region_name=self.region_name)
        client = session.client("sns", region_name=self.region_name)
        print("*" * 80)
        print("TODO")
        print(client)
        print(notification)
        print("TODO")
        print("*" * 80)


#        client.publish(
#            TopicArn=self.sns_topic_arn,
#            Message='string',
#            Subject='string',
#            MessageStructure='string',
#            MessageAttributes={
#                'string': {
#                    'DataType': 'string',
#                    'StringValue': 'string',
#                    'BinaryValue': b'bytes'
#                }
#            },
#            MessageDeduplicationId='string',
#            MessageGroupId='string'
#        )
