import json
from lambdas.publish_cloudwatch_metrics_lambda_handler import *


def lambda_handler(event, context):
    LoggerUtility.setLevel()
    txt=json.dumps(event[0])
    batch_id = json.loads(txt).get("batchId")
    if batch_id is None:
        return event 
    publish_cloudwatch_metrics(event, context, batch_id)
    return event
