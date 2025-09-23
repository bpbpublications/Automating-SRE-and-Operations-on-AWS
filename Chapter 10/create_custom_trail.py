
import boto3
from datetime import datetime


def main():
    trail_name = 'custom_trail'
    bucket_name = 'my-trail-logs'
    create_custom_trail(trail_name, bucket_name)
    create_athena_db(bucket_name)


def create_custom_trail(trail_name, bucket_name):

    # Create CloudTrail client
    cloudtrail = boto3.client('cloudtrail')

    try:
        # Create the trail
        response = cloudtrail.create_trail(
            Name=trail_name,
            S3BucketName=bucket_name,
            IsMultiRegionTrail=True,
            EnableLogFileValidation=True
        )

        # Start logging for the trail
        cloudtrail.start_logging(Name=trail_name)

        print(f"Successfully created trail: {trail_name}")
        return response
    except Exception as e:
        print(f"Error creating trail: {str(e)}")
        raise


def create_athena_db(bucket_name):
    # Create STS client to get the account_id
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    database_name = 'cloudtrail_logs'
    table_name = 'cloudtrail_events'

    s3_bucket_location = f's3://{bucket_name}/AWSLogs/{account_id}/CloudTrail'
    # Create Athena client
    athena = boto3.client('athena')

    # Create database if not exists
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"

    # Create table for CloudTrail logs
    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
        eventVersion STRING,
        userIdentity STRUCT<
            type: STRING,
            principalId: STRING,
            arn: STRING,
            accountId: STRING,
            userName: STRING>,
        eventTime STRING,
        eventSource STRING,
        eventName STRING,
        awsRegion STRING,
        sourceIpAddress STRING,
        userAgent STRING,
        requestParameters STRING,
        responseElements STRING,
        errorCode STRING,
        errorMessage STRING,
        requestId STRING,
        eventId STRING,
        eventType STRING
    )
    PARTITIONED BY (region string, year string, month string, day string)
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '{s3_bucket_location}'
    TBLPROPERTIES (
        'projection.enabled'='true',
        'projection.region.type'='enum',
        'projection.region.values'='us-east-1,us-west-2',
        'projection.year.type'='integer',
        'projection.year.range'='2024,2025',
        'projection.year.digits'='4',
        'projection.month.type'='integer',
        'projection.month.range'='1,12',
        'projection.month.digits'='2',
        'projection.day.type'='integer',
        'projection.day.range'='1,31',
        'projection.day.digits'='2',
        'storage.location.template'='{s3_bucket_location}/${{region}}/${{year}}/${{month}}/${{day}}'
    )
    """

    # Query to repair table partitions
    repair_table_query = f"MSCK REPAIR TABLE {database_name}.{table_name}"

    # Execute each of the queries one by one
    try:
        response = athena.start_query_execution(
            QueryString=create_database_query,
            ResultConfiguration={
                'OutputLocation': f'{s3_bucket_location}/athena_results/'}
        )

        response = athena.start_query_execution(
            QueryString=create_table_query,
            ResultConfiguration={
                'OutputLocation': f'{s3_bucket_location}/athena_results/'}
        )

        response = athena.start_query_execution(
            QueryString=repair_table_query,
            ResultConfiguration={
                'OutputLocation': f'{s3_bucket_location}/athena_results/'}
        )

        print(f"Successfully set up Athena table: {table_name}")
        return response
    except Exception as e:
        print(f"Error setting up Athena: {str(e)}")
        raise


if __name__ == "__main__":
    main()
