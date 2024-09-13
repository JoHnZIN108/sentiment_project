#!/usr/bin/env python3

import aws_cdk as cdk

from sentiment_project.sentiment_project_stack import SentimentProjectStack


app = cdk.App()
SentimentProjectStack(app, "SentimentProjectStack")

app.synth()
