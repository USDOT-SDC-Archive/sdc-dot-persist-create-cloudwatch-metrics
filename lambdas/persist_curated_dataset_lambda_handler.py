from common.logger_utility import *
from concurrent.futures import ThreadPoolExecutor
import functools
from common.redshift import RedshiftManager, RedshiftConnection
from common.template_loader import TemplateLoader
from root import PROJECT_DIR
import boto3
from boto3.dynamodb.conditions import Attr, Key
from base64 import *

ENCRYPTED_REDSHIFT_MASTER_PASSWORD = os.environ['REDSHIFT_MASTER_PASSWORD']
DECRYPTED_REDSHIFT_MASTER_PASSWORD = \
boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_REDSHIFT_MASTER_PASSWORD))['Plaintext'].decode('utf-8')
REDSHIFT_SQL_DIR = os.path.join(PROJECT_DIR, 'redshift_sql')


def __publish_persist_records_to_cloudwath(table_name, batch_id, is_historical):
    try:
        LoggerUtility.logInfo("Started querying elt_run_state_stats for batch {} ".format(batch_id))
        redshift_manager = __make_redshift_manager()
        dw_schema_name = "dw_waze"
        elt_schema_name = "elt_waze"

        if (is_historical):
            dw_schema_name = "dw_waze_history"
            elt_schema_name = "elt_waze_history"
        cursor = redshift_manager.execute_from_file("get_state_stats.sql",
                                                    batchIdValue=batch_id,
                                                   dw_schema_name=dw_schema_name,
                                                   elt_schema_name=elt_schema_name)
        __publishCustomMetricsToCloudwatch(cursor)

    except Exception as e:
        LoggerUtility.logInfo("Failed to persist get status for batch "
                              " - {} with exception - {}".format(batch_id, e))
        raise


def __publish_pre_persist_records_to_cloudwath(table_name, batch_id, is_historical):
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table(os.environ["CURATION_MANIFEST_TABLE"]) # dev-CurationManifestFilesTable, dev-BatchId-TableName-index
        response = table.query(
            IndexName=os.environ["CURATION_MANIFEST_TABLE_BATCH_INDX"],
            KeyConditionExpression=Key('BatchId').eq(batch_id),
            FilterExpression=Attr('IsHistorical').eq(is_historical))
        for item in response['Items']:
            totalCuratedRecordsByState = item["TotalCuratedRecordsByState"]
            __publishPrePersistCustomMetricsToCloudwatch(item["TableName"], totalCuratedRecordsByState)

    except Exception as e:
        LoggerUtility.logInfo("Failed to persist get status for batch "
                              " - {} with exception - {}".format(batch_id, e))
        raise


def __publishPrePersistCustomMetricsToCloudwatch(tableName, totalCuratedRecordsByState):
    try:
        for key, value in totalCuratedRecordsByState.items():
            cloudwatch_client = boto3.client('cloudwatch')
            cloudwatch_client.put_metric_data(
                Namespace='dot-sdc-waze-pre-persistence-metric',
                MetricData=[
                    {
                        'MetricName': 'Counts by state and traffic type',
                        'Dimensions': [
                            {
                                'Name': 'State',
                                'Value': key
                            },
                            {
                                'Name': 'TrafficType',
                                'Value': tableName
                            }
                        ],
                        'Value': value,
                        'Unit': 'Count'
                    },
                ]
            )
    except Exception as e:
        print("Failed to publish custom cloudwatch metrics")
        raise e


def __publishCustomMetricsToCloudwatch(totalRowsIngested):
    try:
        for record in totalRowsIngested:
            cloudwatch_client = boto3.client('cloudwatch')
            cloudwatch_client.put_metric_data(
                Namespace='dot-sdc-waze-persistence-metric',
                MetricData=[
                    {
                        'MetricName': 'Counts by state and traffic type',
                        'Dimensions': [
                            {
                                'Name': 'State',
                                'Value': record[0]
                            },
                            {
                                'Name': 'TrafficType',
                                'Value': record[1]
                            }
                        ],
                        'Value': record[2],
                        'Unit': 'Count'
                    },
                ]
            )
    except Exception as e:
        print("Failed to publish custom cloudwatch metrics")
        raise e


def __make_redshift_manager():
    redshift_connection = RedshiftConnection(
        os.environ['REDSHIFT_MASTER_USERNAME'],
        DECRYPTED_REDSHIFT_MASTER_PASSWORD,
        os.environ['REDSHIFT_JDBC_URL']
    )
    query_loader = TemplateLoader(REDSHIFT_SQL_DIR)
    return RedshiftManager(
        region_name='us-east-1',
        redshift_role_arn=os.environ['REDSHIFT_ROLE_ARN'],
        redshift_connection=redshift_connection,
        query_loader=query_loader
    )


def persist_curated_datasets(event, context, batch_id, tableName, is_historical):
    LoggerUtility.setLevel()

    __publish_pre_persist_records_to_cloudwath(tableName, batch_id, is_historical)
    __publish_persist_records_to_cloudwath(tableName, batch_id, is_historical)