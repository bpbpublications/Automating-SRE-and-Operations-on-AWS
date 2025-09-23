import boto3
import json


def main():
    config_client = boto3.client('config', region_name='us-west-2')

    response = config_client.put_config_rule(
        ConfigRule={
            'ConfigRuleName': 'required-tags-rule',
            'Description': 'Checks if EC2 instances have required tags',
            'Scope': {
                'ComplianceResourceTypes': [
                    'AWS::EC2::Instance',
                ]
            },
            'Source': {
                'Owner': 'CUSTOM_LAMBDA',
                'SourceIdentifier': 'arn:aws:lambda:us-west-2:867470678740:function:config_check',
                'SourceDetails': [
                    {
                        'EventSource': 'aws.config',
                        'MessageType': 'ConfigurationItemChangeNotification'
                    }
                ]
            },
            'InputParameters': json.dumps({
                'requiredTags': 'Environment,Project,Owner'
            }),
            'ConfigRuleState': 'ACTIVE'
        }
    )
    return response


if __name__ == "__main__":
    main()
