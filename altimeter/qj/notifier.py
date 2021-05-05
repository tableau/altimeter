"""ResultSetNotifier - sends SNS messages when a result set is created"""
from dataclasses import dataclass
import json

import boto3

from altimeter.qj import schemas


@dataclass(frozen=True)
class ResultSetNotifier:
    sns_topic_arn: str
    region_name: str

    def notify(self, notification: schemas.ResultSetNotification) -> None:
        session = boto3.Session(region_name=self.region_name)
        sns_client = session.client("sns", region_name=self.region_name)
        sns_client.publish(
            TopicArn=self.sns_topic_arn,
            Message=json.dumps({"default": notification.json()}),
            MessageStructure="json",
        )
