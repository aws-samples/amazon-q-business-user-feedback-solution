#!/usr/bin/env python3
import os

from aws_cdk import App, Tags,Aspects,Environment,Aws
from lambda_stack.QLambda_stack import QLambdaStack
from cdk_nag import AwsSolutionsChecks, NagSuppressions

app = App()


qlambda = QLambdaStack(app, "dev-businessq-analytics-stack", description='Feedback collection for Amazon Q chatbot.')
Tags.of(qlambda).add("project", "businessq-analytics-stack")

NagSuppressions.add_stack_suppressions(
    qlambda,
    [
        {
            "id": "AwsSolutions-S1",
            "reason": "S3 Access Logs are disabled for demo purposes.",
        },
        {
            "id": "AwsSolutions-L1",
            "reason": "Boto version requires python 3.11",
        },

        {
            "id": "AwsSolutions-IAM4",
            "reason": "Use Lambda managed policy with Lambda for custom policies. ",
        },
        {
            "id": "AwsSolutions-IAM5",
            "reason": "Using CDK S3 grant write permissions.",
        },
        {
            "id": "AwsSolutions-SQS3",
            "reason": "DLQ not used for Glue crawler for sample.",
        },
        {
            "id": "AwsSolutions-ATH1",
            "reason": " Athena workgroup uses SSE_S3 encryption.",
        }
    ],
)

Aspects.of(app).add(AwsSolutionsChecks())

app.synth()
