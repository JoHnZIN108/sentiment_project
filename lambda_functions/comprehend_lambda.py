import boto3
import json
import os
import base64

# Initilizing clients
comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')
kinesis = boto3.client('kinesis')

# Fetching table name from environment variables
table_name = os.environ.get('TABLE_NAME')
DB_table = dynamodb.Table(table_name)

def lambda_handler(event, context):
  for record in event['Records']:
    # Decoding base 64
    data = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))

    # Sentiment analysis
    response = comprehend.detect_sentiment(Text=data['text'], LanguageCode='en')

    # Extracting sentiment score
    sentiment = response['Sentiment']
    sentiment_score = response['SentimentScore']

    try:
    # Soring in DynamoDB
      DB_table.put(
        Item={
          'comment_id': record['eventID'],
          'timestamp': data['published_at'],
          'author': data['author'],
          'text': data['text'],
          'like_count': data['like_count'],
          'sentiment': sentiment,
          'sentiment_score': sentiment_score
        }
      )

      return {
        'statusCode': 200,
        'body': json.dumps('Comments uploaded to DynamoDB successfully')
      }
    
    except Exception as e:
      return {
        'statusCode': 500,
        'body': json.dumps(f'Error uploading comments to DynamoDB: {e}')
      }