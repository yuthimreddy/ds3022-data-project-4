import os
import json
import boto3
from chalice import Chalice
from botocore.exceptions import ClientError

app = Chalice(app_name='s3-events')
app.debug = True

# Set the values in the .chalice/config.json file.
S3_BUCKET = os.environ.get('APP_BUCKET_NAME', '')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', '')

@app.on_s3_event(bucket=S3_BUCKET, events=['s3:ObjectCreated:*'], suffix='.json')
def s3_handler(event):
    app.log.debug(f"Received S3 event: {event}, key: {event.key}")
    
    # 1. Get the data
    data = get_s3_object(event.bucket, event.key)
    
    # 2. Insert the data (You were missing this call in your original code)
    if data:
        insert_data_into_dynamodb(data)

def get_s3_object(bucket, key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        app.log.debug(f"Retrieved S3 object data: {data}")
        return data
    except ClientError as e:
        app.log.error(f"Error getting S3 object: {e}")
        raise e

def insert_data_into_dynamodb(data):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    try:
        # Ensure your JSON file actually contains these specific keys
        response = table.put_item(
            Item={
                'event_key': data['event_key'],
                'building_code': data['building_code'],
                'building_door_id': data['building_door_id'],
                'access_time': data['access_time'],
                'user_identity': data['user_identity']
            }
        )
        app.log.debug(f"DynamoDB response: {response}")
        return response
    except ClientError as e:
        app.log.error(f"Error inserting data into DynamoDB: {e}")
        raise e
    except KeyError as e:
        app.log.error(f"Missing key in JSON data: {e}")
        raise e

@app.route('/access', methods=['GET'])
def get_access():
    dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table(DYNAMODB_TABLE_NAME) 
    
    try: 
        # Note: 'scan' can be slow on large tables. Consider 'query' if possible.
        response = table.scan()
        items = response.get('Items', [])
        
        # Sort items; using .get ensures it doesn't crash if access_time is missing
        sorted_items = sorted(items, key=lambda x: x.get('access_time', ''))
        
        return sorted_items
    except ClientError as e:
        app.log.error(f"Error retrieving data from DynamoDB: {e}")
        raise e