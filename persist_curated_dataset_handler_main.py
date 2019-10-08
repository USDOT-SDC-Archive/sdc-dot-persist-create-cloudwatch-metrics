import json

from lambdas.persist_curated_dataset_lambda_handler import *


def lambda_handler(event, *args, **kwargs):
    LoggerUtility.set_level()
    txt = json.dumps(event[0])
    batch_id = json.loads(txt).get("batchId")
    is_historical = json.loads(txt).get("is_historical") == "true"
    if batch_id is None:
        return event
    table_name = json.loads(txt).get("tablename")
    persist_curated_datasets(batch_id, table_name, is_historical)
    return event
