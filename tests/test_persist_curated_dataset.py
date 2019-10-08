import os
from unittest import mock

import boto3
import botocore
import pytest
from moto import mock_cloudwatch
from moto import mock_dynamodb

from lambdas import persist_curated_dataset_lambda_handler


def test__make_redshift_manager(monkeypatch):
    os.environ['REDSHIFT_MASTER_PASSWORD'] = "super secret"
    os.environ['REDSHIFT_MASTER_USERNAME'] = "tester"
    os.environ['REDSHIFT_JDBC_URL'] = "www.url.com"
    os.environ['REDSHIFT_ROLE_ARN'] = "redshift_role_arn"

    class MockBoto3Client:
        def __init__(*args, **kwargs):
            pass

        @staticmethod
        def decrypt(*args, **kwargs):
            return {'Plaintext': os.environ['REDSHIFT_MASTER_PASSWORD'].encode('utf-8')}

    def mock_b64decode(*args, **kwargs):
        pass

    class MockRedshiftConnection:

        def __init__(self, user, password, redshift_jdbc_url):
            self.user = user
            self.password = password
            self.redshift_jdbc_url = redshift_jdbc_url

    class MockRedshiftManager:

        def __init__(self, region_name, redshift_role_arn, redshift_connection, query_loader):
            self.region_name = region_name
            self.redshift_role_arn = redshift_role_arn
            self.redshift_connection = redshift_connection
            self.query_loader = query_loader

    # try overloading TemplateLoader init with monkeypatch and see what happens
    def mock_template_loader(*args, **kwargs):
        return

    monkeypatch.setattr(boto3, "client", MockBoto3Client)

    persist_curated_dataset_lambda_handler.b64decode = mock_b64decode
    persist_curated_dataset_lambda_handler.RedshiftConnection = MockRedshiftConnection
    persist_curated_dataset_lambda_handler.RedshiftManager = MockRedshiftManager
    persist_curated_dataset_lambda_handler.TemplateLoader = mock_template_loader

    redshift_manager = persist_curated_dataset_lambda_handler.__make_redshift_manager()

    assert redshift_manager.region_name == 'us-east-1'
    assert redshift_manager.redshift_role_arn == os.environ['REDSHIFT_ROLE_ARN']
    assert redshift_manager.redshift_connection.user == os.environ['REDSHIFT_MASTER_USERNAME']
    assert redshift_manager.redshift_connection.password == os.environ['REDSHIFT_MASTER_PASSWORD']
    assert redshift_manager.redshift_connection.redshift_jdbc_url == os.environ['REDSHIFT_JDBC_URL']
    assert redshift_manager.query_loader == mock_template_loader()


@mock_cloudwatch
def test__publish_custom_metrics_to_cloudwatch():
    with pytest.raises(IndexError):
        total_rows_ingested = [['NH', 'traffic type 1', 1], ['MA', 'traffic type 2', 2], ['CO', 'traffic type 3']]
        persist_curated_dataset_lambda_handler.__publish_custom_metrics_to_cloudwatch(total_rows_ingested)


@mock_cloudwatch
def test__publish_pre_persist_custom_metrics_to_cloudwatch():
    with pytest.raises(botocore.exceptions.ParamValidationError):
        total_curated_records_by_state = {
            "key1": 1,
            "key2": "2",
        }

        persist_curated_dataset_lambda_handler.__publish_pre_persist_custom_metrics_to_cloudwatch(
            "table_name",
            total_curated_records_by_state
        )


@mock_dynamodb
def test__publish_pre_persist_records_to_cloudwatch(monkeypatch):
    os.environ["CURATION_MANIFEST_TABLE"] = 'dev-CurationManifestFilesTable'
    os.environ["CURATION_MANIFEST_TABLE_BATCH_INDX"] = "random_index_name"

    table_name = "tablename"
    total_curated_records_by_state = 16

    class MockDynamodb:
        class Table:
            def __init__(self, *args, **kwargs):
                pass

            def query(self, IndexName, KeyConditionExpression, FilterExpression):
                return {
                    "Items": [{
                        "TotalCuratedRecordsByState": total_curated_records_by_state,
                        "TableName": table_name
                    }]
                }

    def mock_boto3_resource(*args, **kwargs):
        dynamodb = MockDynamodb()
        return dynamodb

    monkeypatch.setattr(boto3, "resource", mock_boto3_resource)

    persist_curated_dataset_lambda_handler.__publish_pre_persist_custom_metrics_to_cloudwatch = mock.MagicMock()

    persist_curated_dataset_lambda_handler.__publish_pre_persist_records_to_cloudwatch(
        table_name="this param is not currently used", batch_id=11111, is_historical=True
    )

    persist_curated_dataset_lambda_handler.__publish_pre_persist_custom_metrics_to_cloudwatch.assert_called_with(
        table_name, total_curated_records_by_state
    )


def test__publish_persist_records_to_cloudwatch_historical():
    batch_id = 00000
    is_historical = True

    class MockRedshiftManager:

        @staticmethod
        def execute_from_file(*args, **kwargs):
            return kwargs

    def mock_make_redshift_manager():
        return MockRedshiftManager()

    persist_curated_dataset_lambda_handler.__publish_custom_metrics_to_cloudwatch = mock.MagicMock()
    persist_curated_dataset_lambda_handler.__make_redshift_manager = mock_make_redshift_manager

    persist_curated_dataset_lambda_handler.__publish_persist_records_to_cloudwatch(batch_id, is_historical)

    persist_curated_dataset_lambda_handler.__publish_custom_metrics_to_cloudwatch.assert_called_with(
        {
            "batchIdValue": batch_id,
            "dw_schema_name": "dw_waze_history",
            "elt_schema_name": "elt_waze_history"
        }
    )


def test__publish_persist_records_to_cloudwatch_not_historical():
    batch_id = 00000
    is_historical = False

    class MockRedshiftManager:

        @staticmethod
        def execute_from_file(*args, **kwargs):
            return kwargs

    def mock_make_redshift_manager():
        return MockRedshiftManager()

    persist_curated_dataset_lambda_handler.__publish_custom_metrics_to_cloudwatch = mock.MagicMock()
    persist_curated_dataset_lambda_handler.__make_redshift_manager = mock_make_redshift_manager

    persist_curated_dataset_lambda_handler.__publish_persist_records_to_cloudwatch(batch_id, is_historical)

    persist_curated_dataset_lambda_handler.__publish_custom_metrics_to_cloudwatch.assert_called_with(
        {
            "batchIdValue": batch_id,
            "dw_schema_name": "dw_waze",
            "elt_schema_name": "elt_waze"
        }
    )
