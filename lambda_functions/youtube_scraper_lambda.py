import boto3
import json
import googleapiclient.discovery
import pandas as pd
import os

api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = ""

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY)

region= os.environ.get('AWS_REGION')

# setting up kinesis client
kinesis = boto3.client('kinesis', region_name=region)

def lambda_handler(event, context):
  
  video_id = "WNrB1Q9Rry0"
  max_result=10 # number of comments scrapped

  try: 
    request = youtube.commentThreads().list(
      part="snippet",
      videoId=video_id,
      maxResults=max_result
    )

    response = request.execute()

    # Sending scrapped comments to kinesis Data Stream
    for item in response['items']:
      comment = item['snippet']['toplevelComment']['snippet']
      comment_data = {
          'author': comment['authorDisplayName'],
          'published_at': comment['publishedAt'],
          'updated_at': comment['updatedAt'],
          'like_count': comment['likeCount'],
          'text': comment['textDisplay']
      }

      kinesis.put_record(
        StreamName='YouTubeCommentStream',
        Data=json.dumps(comment_data),
        PartitionKey='partitionkey'
      )

      return {
        'statusCode': 200,
        'body':json.dumps('Comments uploaded to kinesis stream successfully')
      }
    
  except Exception as e:
    print(e)
    return {
      'statusCode': 500,
      'body': json.dumps(f'Error uploading comments to kinesis stream: {e}')
    }
