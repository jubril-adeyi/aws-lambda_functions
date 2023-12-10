import boto3
import datetime
import os
import json

REGION_NAME = os.environ.get("REGION")
SOURCE_EMAIL= os.environ.get("SOURCE_EMAIL")
DESTINATION_EMAIL = os.environ.get("DESTINATION_")

ses = boto3.client('ses')


def is_capacity_zero(table_name, cloudwatch_client, dynamodb_client, days):
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(days=days)

    dynamodb_response = dynamodb_client.describe_table(
        TableName=table_name
    )

    table_details = dynamodb_response.get('Table')
    if not table_details:
        return False  

    billing_mode_summary = table_details.get('BillingModeSummary')
    if not billing_mode_summary:
        return False  

    billing_mode = billing_mode_summary.get('BillingMode')
    if not billing_mode:
        return False  

    if billing_mode != 'PROVISIONED':
        return False  # Skip if the table is not provisioned

    # Replace 'your-namespace' with the appropriate CloudWatch namespace for DynamoDB
    cloudwatch_response = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'rcu',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/DynamoDB',  # CloudWatch namespace for DynamoDB
                        'MetricName': 'ConsumedReadCapacityUnits',
                        'Dimensions': [
                            {
                                'Name': 'TableName',
                                'Value': table_name
                            }
                        ]
                    },
                    'Period': 86400,  # 1 day in seconds
                    'Stat': 'Sum'
                },
                'ReturnData': True,
                'Label': 'rcu'
            },
            {
                'Id': 'wcu',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/DynamoDB',  # CloudWatch namespace for DynamoDB
                        'MetricName': 'ConsumedWriteCapacityUnits',
                        'Dimensions': [
                            {
                                'Name': 'TableName',
                                'Value': table_name
                            }
                        ]
                    },
                    'Period': 86400,  # 1 day in seconds
                    'Stat': 'Sum'
                },
                'ReturnData': True,
                'Label': 'wcu'
            }
        ],
        StartTime=start_time,
        EndTime=end_time
    )

    if 'rcu' in cloudwatch_response['MetricDataResults'][0]['Timestamps']:
        return False  # Non-zero RCU data found in the last 'days' days
    if 'wcu' in cloudwatch_response['MetricDataResults'][1]['Timestamps']:
        return False  # Non-zero WCU data found in the last 'days' days

    return True  # Both RCU and WCU were zero for the last 'days' days

def notify(changed_tables):
    print("The following DynamoDB tables were changed to On-Demand:")
    for table in changed_tables:
        print(f"- {table}")

def lambda_handler(event, context):
    session = boto3.session.Session()
    dynamodb_client = session.client(
        service_name='dynamodb',
        region_name=REGION_NAME
    )
    cloudwatch_client = session.client(
        service_name= 'cloudwatch',
        region_name=REGION_NAME
    )
    ses = boto3.client('ses')

    changed_tables = []

    table_list = dynamodb_client.list_tables()['TableNames']

    for table_name in table_list:
        if is_capacity_zero(table_name, cloudwatch_client, dynamodb_client, days=90):
            print(table_name)
            dynamodb_client.update_table(
                TableName=table_name,
                BillingMode='PAY_PER_REQUEST'
            )
            changed_tables.append(table_name)

    if changed_tables:
        send_summary_notification(changed_tables)
        notify(changed_tables)
    else:
        print("No Unused Provisioned Tables were changed to on-demand")


def send_summary_notification(changed_tables):
   
    ses_data = {}
    
    ses_data["table_names"] = []
    for table_name in changed_tables:
        ses_data["table_names"].append(table_name)

    if ses_data:
        print(json.dumps(ses_data))
        ses.send_templated_email(
            Source=f"{SOURCE_EMAIL}",
            Destination={"ToAddresses": [f"{DESTINATION_EMAIL}"]},
            ReplyToAddresses=[f"{DESTINATION_EMAIL}"],
            Template=os.environ.get("DYNAMODB_COST_OPTIMIZATION_EMAIL_TEMPLATE_NAME"),
            TemplateData=json.dumps(ses_data),
        )