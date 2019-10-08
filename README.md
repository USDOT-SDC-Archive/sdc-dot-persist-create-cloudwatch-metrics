
[![Build Status](https://travis-ci.org/usdot-jpo-sdc-projects/sdc-dot-persist-create-cloudwatch-metrics.svg?branch=master)](https://travis-ci.org/usdot-jpo-sdc-projects/sdc-dot-persist-create-cloudwatch-metrics)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=usdot-jpo-sdc-projects_sdc-dot-persist-create-cloudwatch-metrics&metric=alert_status)](https://sonarcloud.io/dashboard?id=usdot-jpo-sdc-projects_sdc-dot-persist-create-cloudwatch-metrics)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=usdot-jpo-sdc-projects_sdc-dot-persist-create-cloudwatch-metrics&metric=coverage)](https://sonarcloud.io/dashboard?id=usdot-jpo-sdc-projects_sdc-dot-persist-create-cloudwatch-metrics)
# sdc-dot-persist-create-cloudwatch-metrics
This lambda function is responsible for publishing the records in the custom cloudwatch metrics.

<a name="toc"/>

## Table of Contents

[I. Release Notes](#release-notes)

[II. Overview](#overview)

[III. Design Diagram](#design-diagram)

[IV. Getting Started](#getting-started)

[V. Unit Tests](#unit-tests)

[VI. Support](#support)

---

<a name="release-notes"/>


## [I. Release Notes](ReleaseNotes.md)
TO BE UPDATED

<a name="overview"/>

## II. Overview
The primary functions that this lambda function serves:
* **publish_pre_persist_records_to_cloudwath** - publishes the count of total curated records by state for each traffic type from the manifest table in a custom metric in cloudwatch. 
* **publish_persist_records_to_cloudwath** - publishes the count of persisted records in redshift by querying the elt_run_state_stats for a particular batch id, state and for each traffic type in a custom metric in cloudwatch.

<a name="design-diagram"/>

## III. Design Diagram

![sdc-dot-persist-create-cloudwatch-metrics](images/waze-data-persistence.png)

<a name="getting-started"/>

## IV. Getting Started

The following instructions describe the procedure to build and deploy the lambda.

### Prerequisites
* NA 

---
### ThirdParty library

*NA

### Licensed softwares

*NA

### Programming tool versions

*Python 3.6


---
### Build and Deploy the Lambda

#### Environment Variables
Below are the environment variables needed :- 

REDSHIFT_MASTER_PASSWORD - {master_password_for_redshift}

DDB_MANIFEST_TABLE_ARN - {arn_of_manifest_table_in_dynamodb}

DDB_CURATED_RECORDS_INDEX_NAME - {index_name_of_curated_records_in_dynamodb}

REDSHIFT_MASTER_USERNAME - {master_username_for_redshift}

REDSHIFT_JDBC_URL - {jdbc_url_for_redshift}

REDSHIFT_ROLE_ARN - {role_arn_for_redshift}

#### Build Process

**Step 1**: Setup virtual environment on your system by foloowing below link
https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python

**Step 2**: Create a script with below contents e.g(sdc-dot-persist-create-cloudwatch-metrics.sh)
```#!/bin/sh

cd sdc-dot-persist-create-cloudwatch-metrics
zipFileName="sdc-dot-persist-create-cloudwatch-metrics.zip"

zip -r9 $zipFileName common/*
zip -r9 $zipFileName lambdas/*
zip -r9 $zipFileName redshift_sql/*
zip -r9 $zipFileName README.md
zip -r9 $zipFileName persist_curated_dataset_handler_main.py
zip -r9 $zipFileName root.py
```

**Step 3**: Change the permission of the script file

```
chmod u+x sdc-dot-persist-create-cloudwatch-metrics.sh
```

**Step 4** Run the script file
./sdc-dot-persist-create-cloudwatch-metrics.sh

**Step 5**: Upload the sdc-dot-persist-create-cloudwatch-metrics.zip generated from Step 4 to a lambda function via aws console.

[Back to top](#toc)

---
<a name="unit-tests"/>

## V. Unit Tests

TO BE UPDATED

---
<a name="support"/>

## VI. Support

For any queries you can reach to support@securedatacommons.com
---
[Back to top](#toc)

