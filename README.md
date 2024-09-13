# Overview
This original project is a serveless solution designed to analyze the sentiment of YouTube video comments using AWS services. The project leverages AWS CDK (Cloud Development Kit) for Infrastructure as Code (IaC), making it easy to deploy and manage cloud resources. The architecture involves extracting comments from YouTube, processing them with AWS Comprehend for sentiment analysis, and storing the results in DynamoDB.This project is a sentiment analysis solution built using AWS services.

# Piple/AWS Architecture
![image](https://github.com/user-attachments/assets/dd19dc85-32f1-4fc8-834a-030b18f2d2c3)


# How it Works
The youtube_scraper_lambda.py Lambda function uses the YouTube Data API to fetch comments from a specified video.
The function is triggered every 5 minutes by an EventBridge rule.
Extracted comments are sent to an AWS Kinesis Data Stream.
Sentiment Analysis:

The comprehend_lambda.py Lambda function is triggered by new records in the Kinesis Data Stream.
It processes each comment using AWS Comprehend to determine the sentiment.
The sentiment analysis results are stored in an AWS DynamoDB table.
Infrastructure:

The AWS CDK is used to define and deploy the infrastructure components, including Lambda functions, Kinesis Data Stream, EventBridge rules, and DynamoDB tables.

# Notes
Before deploying, you need to install and zip the youtube_scraper_lambda.py with the pandas and google-api-python-client dependencies.
