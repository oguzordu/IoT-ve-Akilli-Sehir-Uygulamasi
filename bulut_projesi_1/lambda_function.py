import json
import os
import boto3 
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'SmartCityData') 
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def lambda_handler(event, context):
    """
    This function is triggered by an AWS IoT Rule.
    It receives the MQTT message payload from the event,
    and puts it into a DynamoDB table.
    """
    print(f"Received event: {json.dumps(event)}")
    try:
        if 'deviceId' not in event or 'timestamp' not in event:
            print("Error: 'deviceId' or 'timestamp' not in event payload.")
            return {
                'statusCode': 400,
                'body': json.dumps("'deviceId' and 'timestamp' are required in the payload")
            }

        print(f"Attempting to put item into DynamoDB table: {DYNAMODB_TABLE_NAME}")
        print(f"Item: {json.dumps(event)}")
        
        response = table.put_item(Item=event)
        
        print(f"Successfully put item into DynamoDB. Response: {json.dumps(response)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data successfully processed and stored in DynamoDB')
        }

    except Exception as e:
        print(f"Error processing event or putting item to DynamoDB: {str(e)}")
        
        
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
