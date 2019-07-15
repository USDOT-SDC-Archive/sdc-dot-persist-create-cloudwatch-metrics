from common.logger_utility import *
from concurrent.futures import ThreadPoolExecutor
import functools
from common.redshift import RedshiftManager, RedshiftConnection
from common.template_loader import TemplateLoader
from root import PROJECT_DIR
import boto3
from boto3.dynamodb.conditions import Attr, Key
from base64 import *


def __publish_persist_records_to_cloudwatch(table_name, batch_id, is_historical):
    """
    Publish persist records to cloudwatch
    :param table_name: Not used
    :param batch_id:
    :param is_historical:
    :return:
    """
    try:
        LoggerUtility.logInfo("Started querying elt_run_state_stats for batch {} ".format(batch_id))

        # create a redshift manager instance
        redshift_manager = __make_redshift_manager()

        # set the dw and elt schema names
        dw_schema_name = "dw_waze"
        elt_schema_name = "elt_waze"
        if is_historical:
            dw_schema_name = "dw_waze_history"
            elt_schema_name = "elt_waze_history"

        # gather data from the query get_state_stats.sql
        cursor = redshift_manager.execute_from_file("get_state_stats.sql", batchIdValue=batch_id,
                                                    dw_schema_name=dw_schema_name, elt_schema_name=elt_schema_name)

        # publish the metrics to cloudwatch
        __publish_custom_metrics_to_cloudwatch(cursor)

    except Exception as e:
        LoggerUtility.logInfo("Failed to persist get status for batch "
                              " - {} with exception - {}".format(batch_id, e))
        raise


def __publish_pre_persist_records_to_cloudwatch(table_name, batch_id, is_historical):
    """

    :param table_name: Not used
    :param batch_id:
    :param is_historical:
    :return:
    """
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table(
            os.environ["CURATION_MANIFEST_TABLE"])  # dev-CurationManifestFilesTable, dev-BatchId-TableName-index

        filter_expression = Attr('IsHistorical')
        filter_expression = filter_expression.eq(is_historical)
        response = table.query(
            IndexName=os.environ["CURATION_MANIFEST_TABLE_BATCH_INDX"],
            KeyConditionExpression=Key('BatchId').eq(batch_id),
            FilterExpression=filter_expression)
        for item in response['Items']:
            total_curated_records_by_state = item["TotalCuratedRecordsByState"]
            __publish_pre_persist_custom_metrics_to_cloudwatch(item["TableName"], total_curated_records_by_state)

    except Exception as e:
        LoggerUtility.logInfo("Failed to persist get status for batch "
                              " - {} with exception - {}".format(batch_id, e))
        raise


def __publish_pre_persist_custom_metrics_to_cloudwatch(table_name, total_curated_records_by_state):
    """

    :param table_name:
    :param total_curated_records_by_state:
    :return:
    """
    try:
        for key, value in total_curated_records_by_state.items():
            cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
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
                                'Value': table_name
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


def __publish_custom_metrics_to_cloudwatch(total_rows_ingested):
    """
    Publish counts by state and traffic type to cloudwatch
    :param total_rows_ingested: An iterable of iterables with 3 elements (state, traffic type, count)
    :return:
    """
    try:
        for record in total_rows_ingested:
            cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
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
    """
    Create a redshift connection and return a redshift manager with that connection.
    :return:
    """

    # assign and decrypt the redshift master password.
    encrypted_redshift_master_password = os.environ['REDSHIFT_MASTER_PASSWORD']
    decrypted_redshift_master_password = boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(encrypted_redshift_master_password))['Plaintext'].decode('utf-8')
    redshift_sql_dir = os.path.join(PROJECT_DIR, 'redshift_sql')

    redshift_connection = RedshiftConnection(
        os.environ['REDSHIFT_MASTER_USERNAME'],
        decrypted_redshift_master_password,
        os.environ['REDSHIFT_JDBC_URL']
    )
    query_loader = TemplateLoader(redshift_sql_dir)
    return RedshiftManager(
        region_name='us-east-1',
        redshift_role_arn=os.environ['REDSHIFT_ROLE_ARN'],
        redshift_connection=redshift_connection,
        query_loader=query_loader
    )


def persist_curated_datasets(event, context, batch_id, table_name, is_historical):
    LoggerUtility.setLevel()

    __publish_pre_persist_records_to_cloudwatch(table_name, batch_id, is_historical)
    __publish_persist_records_to_cloudwatch(table_name, batch_id, is_historical)
