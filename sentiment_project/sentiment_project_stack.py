from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,\
    aws_lambda as lambda_,
    aws_events as events,
    aws_kinesis as kinesis,
    aws_events_targets as targets,
    aws_sns_subscriptions as subs,
    aws_dynamodb as dynamodb,
    aws_lambda_event_sources as lambda_event_sources,
    RemovalPolicy
)


class SentimentProjectStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Kinesis Data Stream
        kinesis_stream = kinesis.Stream(self, "YouTubeCommentStream", 
                                        stream_name="YouTubeCommentStream",
                                        removal_policy=RemovalPolicy.DESTROY)

        # Dynamodb Table to store sentiment analysis results
        sentiment_table = dynamodb.Table(self, "SentimentTable",
                                         partition_key=dynamodb.Attribute(name="comment_id", type=dynamodb.AttributeType.STRING),
                                         sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING)
                                        )

        # Lambda function for Youtube comment scraping
        youtube_scraper_lambda = lambda_.Function(self, "YoutubeScraper",
                                           runtime=lambda_.Runtime.PYTHON_3_10,
                                           handler="youtube_scraper_lambda.lambda_handler",
                                           code=lambda_.Code.from_asset("lambda_functions/youtube_scraper_lambda.zip"),
                                           timeout=Duration.seconds(300)
                                           )
        
        # Lambda function for sentiment analysis
        comprehend_lambda = lambda_.Function(self, "ComprehendLambda",
                                           runtime=lambda_.Runtime.PYTHON_3_10,
                                           handler="comprehend_lambda.lambda_handler",
                                           code=lambda_.Code.from_asset("lambda_functions"),
                                           environment={
                                               "DYNAMODB_TABLE_NAME": sentiment_table.table_name,
                                               "KINESIS_STREAM_NAME": kinesis_stream.stream_name
                                            },)
        
        # Connecting Kinesis Data Stream as event source for comprehend_lambda
        comprehend_lambda.add_event_source(lambda_event_sources.KinesisEventSource(kinesis_stream,
                                                                      starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                                                                      batch_size=10,
                                                                      retry_attempts=5
                                                                      ))
                                           

        # Create EventBridge rule to invoke youtube_scraper_lambda function every 5 minutes
        rule = events.Rule(self, "Rule",
                           schedule=events.Schedule.rate(Duration.minutes(5)),
                           targets=[targets.LambdaFunction(youtube_scraper_lambda)]
                           )
        
        # Setting IAM role for youtube_scraper lambda function although we don't need to because we are bootstrapped
        kinesis_stream.grant_write(youtube_scraper_lambda)

