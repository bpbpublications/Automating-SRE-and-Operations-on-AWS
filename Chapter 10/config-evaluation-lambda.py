import json
import boto3
import os
from datetime import datetime


def evaluate_compliance(configuration_item, rule_parameters):
    if configuration_item["resourceType"] not in ["AWS::EC2::Instance"]:
        return "NOT_APPLICABLE"

    # Get the required tags from rule parameters
    required_tags = rule_parameters.get("requiredTags", "").split(",")

    # Get the current tags on the instance
    try:
        current_tags = configuration_item["configuration"].get("tags", [])
        current_tag_keys = [tag['key'] for tag in current_tags]

        # Check if all required tags are present
        for tag in required_tags:
            if tag.strip() not in current_tag_keys:
                return "NON_COMPLIANT"
        return "COMPLIANT"

    except Exception as e:
        return "ERROR"


def lambda_handler(event, context):
    # Initialize AWS Config client
    config = boto3.client('config')

    # Parse the invocation event
    invoking_event = json.loads(event['invokingEvent'])
    rule_parameters = json.loads(event.get('ruleParameters', '{}'))

    # Get the configuration item from the invoking event
    configuration_item = invoking_event.get('configurationItem', {})

    # Check if this is a deleted resource
    if configuration_item.get('configurationItemStatus') == 'ResourceDeleted':
        compliance_result = 'NOT_APPLICABLE'
    else:
        compliance_result = evaluate_compliance(configuration_item, rule_parameters)

    # Prepare evaluation response
    evaluation = {
        'ComplianceResourceType': configuration_item['resourceType'],
        'ComplianceResourceId': configuration_item['resourceId'],
        'ComplianceType': compliance_result,
        'OrderingTimestamp': configuration_item['configurationItemCaptureTime']
    }

    if compliance_result == "NON_COMPLIANT":
        evaluation['Annotation'] = f"Missing required tags: {rule_parameters.get('requiredTags')}"

    # Put evaluation results
    try:
        config.put_evaluations(
            Evaluations=[evaluation],
            ResultToken=event['resultToken']
        )
    except Exception as e:
        print(f"Error putting evaluations: {str(e)}")
        raise

    return {
        'compliance_status': compliance_result,
        'resource_id': configuration_item['resourceId']
    }
