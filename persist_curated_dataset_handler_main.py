import json
from common.logger_utility import *
from lambdas.persist_curated_dataset_lambda_handler import *


def lambda_handler(event, context):
    LoggerUtility.setLevel()
    txt=json.dumps(event[0])
    batch_id = json.loads(txt).get("batchId")
    is_historical = json.loads(txt).get("is_historical") == "true"
    if batch_id is None:
        return event
    tableName = json.loads(txt).get("tablename")
    persist_curated_datasets(event, context, batch_id,tableName,is_historical)
    return event
