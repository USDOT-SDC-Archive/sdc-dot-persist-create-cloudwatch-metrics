import json
from lambdas.persist_curated_dataset_lambda_handler import *


def lambda_handler(event, context):
    LoggerUtility.setLevel()
    txt=json.dumps(event[0])
    batch_id = json.loads(txt).get("batchId")
    if batch_id is None:
        return event
    tableName = json.loads(txt).get("tablename")
    persist_curated_datasets(event, context, batch_id,tableName)
    return event
