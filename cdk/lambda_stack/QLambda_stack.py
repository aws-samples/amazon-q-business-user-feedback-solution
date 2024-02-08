from time import strftime
from constructs import Construct
from aws_cdk import Stack, Duration, CfnOutput, Tags,Aws
from aws_cdk import (
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    aws_cloudtrail as cloudtrail,
    aws_events_targets as targets,
)
from aws_cdk.custom_resources import (
    AwsCustomResource,
    AwsCustomResourcePolicy,
    PhysicalResourceId,
)

class QLambdaStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        

        # Retrieving from_email, to_email and classification from the context
        self.from_email = self.node.try_get_context("from_email")
        self.to_email = self.node.try_get_context("to_email")
        self.modelid = self.node.try_get_context("modelid")
        self.application_id  = self.node.try_get_context("application_id")
        self.classification = self.node.try_get_context("classification")
        
        # Adding consumer lambda with necessary policies and roles
        self.add_consumer_lambda()    


    ##############################################################################
    # Method to add consumer lambda along with necessary permissions and policies
    ##############################################################################
    
    def add_consumer_lambda(self):
        
        # Creating S3 bucket with KMS encryption and blocking all public access
        self.data_bucket = s3.Bucket(self,
        "businessq-analytics",
        encryption=s3.BucketEncryption.KMS_MANAGED,
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        enforce_ssl=True,
        removal_policy=RemovalPolicy.DESTROY)
        
        # associating a Classification tag
        Tags.of(self.data_bucket).add("Classification", self.classification) 
        
        # Defining an IAM policy for the Business Q service with necessary permissions
        policy_statement_q = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[ "qbusiness:ListMessages"],
            resources=[f"arn:aws:qbusiness:{Aws.REGION}:{Aws.ACCOUNT_ID}:application/{self.application_id}"]
        )
        policy_statement_bedrock = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["bedrock:InvokeModel"],
            resources=[f"arn:aws:bedrock:{Aws.REGION}::foundation-model/{self.modelid}"]
        )

        policy_statement_ses = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "ses:SendEmail",
                "ses:SendRawEmail"
            ],
            resources=[f"arn:aws:ses:{Aws.REGION}:{Aws.ACCOUNT_ID}:identity/{self.from_email}"]
        )
        
        # Creating an IAM policy and role for the Lambda
        policy_ses = iam.Policy(
            self,
            'SesLambdaPolicy',
            statements=[policy_statement_ses]
        )
        
        policy_q = iam.Policy(
            self,
            'BusinessqLambdaPolicy',
            statements=[policy_statement_q]
        )    
        policy_bedrock = iam.Policy(
            self,
            'BedrockLambdaPolicy',
            statements=[policy_statement_bedrock]
        )   
        
        self.consumer_role = iam.Role(self, "LambdaBusinessQConsumerRole",assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))
        self.consumer_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))

        # Lambdas and layers for the business Q boto API
        boto_layer = _lambda.LayerVersion(
            self, "boto_python3_11_layer",
            code=_lambda.AssetCode('lambdas/layer/boto_python_layer.zip'),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11])
        
        # Defining and deploying a Lambda function
        self.consumer_lambda = _lambda.Function(self, 'businessq-feedback-processor',
            function_name='businessq_feedback_processor',
            handler='lambda-handler.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset(
                'lambdas/businessq_feedback_processor'),
            timeout=Duration.seconds(240),
            memory_size=256,
            role=self.consumer_role,
            environment={
            'S3_DATA_BUCKET': self.data_bucket.bucket_name,
            'FROM_ADDRESS': self.from_email,
            'TO_ADDRESS': self.to_email,
            'MODELID': self.modelid
            },
            layers=[boto_layer]
            )

        # Assigning permissions to the created Lambda function
        self.data_bucket.grant_write(self.consumer_lambda)
        self.consumer_lambda.add_to_role_policy(policy_statement_bedrock)
        self.consumer_lambda.add_to_role_policy(policy_statement_ses)
        self.consumer_lambda.add_to_role_policy(policy_statement_q)
        
        # Setting up a CloudTrail 'trail' and sending its logs to CloudWatch
        self.trail = cloudtrail.Trail(self, 'BusinessQCloudTrail',
                trail_name='BusinessQCloudTrail')

        # Setting up an EventRule to trigger lambda function on certain conditions
        event_rule = cloudtrail.Trail.on_event(self, "BusinessQCloudWatchEvent",
            target=targets.LambdaFunction(self.consumer_lambda)
        )

        event_rule.add_event_pattern(
            source=["aws.qbusiness"],
            detail_type=["AWS API Call via CloudTrail"],
            detail={
                "eventSource":["qbusiness.amazonaws.com"],
                "eventName": ["PutFeedback"]
            }
        )
        
        # Attaching custom advanced event selectors to the CloudTrail 'trail'
        event_selectors = [
            {
                "Name": "Log all data events on an Amazon Q application",
                "FieldSelectors": [
                    {"Field": "eventCategory", "Equals": ["Data"]},
                    {"Field": "resources.type", "Equals": ["AWS::QBusiness::Application"]}
                ]
            },
            {
                "Name": "Log all data events on an Amazon Q data source",
                "FieldSelectors": [
                    {"Field": "eventCategory", "Equals": ["Data"]},
                    {"Field": "resources.type", "Equals": ["AWS::QBusiness::DataSource"]}
                ]
            },
            {
                "Name": "Log all data events on an Amazon Q index",
                "FieldSelectors": [
                    {"Field": "eventCategory", "Equals": ["Data"]},
                    {"Field": "resources.type", "Equals": ["AWS::QBusiness::Index"]}
                ]
            }
        ]

        # cloudtrail data events for Business Q
        cloudtrail_put_event_selectors = AwsCustomResource(
            self,
            id="CloudTrailPutEventSelectors",
            # log_retention=RetentionDays.ONE_WEEK,
            on_create={
                'service': 'CloudTrail',
                'action': 'putEventSelectors',
                'parameters': {
                    'TrailName':'BusinessQCloudTrail',
                    'AdvancedEventSelectors': event_selectors
                },
                'physical_resource_id': PhysicalResourceId.of("cloudtrail_"+ strftime("%Y%m%d%H%M%S"))
                
            },
            policy=AwsCustomResourcePolicy.from_sdk_calls(
                resources=[f'arn:aws:cloudtrail:{Aws.REGION}:{Aws.ACCOUNT_ID}:trail/BusinessQCloudTrail'])
        )

        # Making an SDK call to verify SES email identity, 
        # which is typically required before SES begins to send emails from or 
        # check the incoming emails to the supplied email address.

        verify_identity = AwsCustomResource(
        self,
        id="VerifyEmailIdentity",
        on_create={
            'action':"VerifyEmailIdentity",
            'service':"SES",  
            'parameters':{
            "EmailAddress": self.from_email
            },
            'physical_resource_id': PhysicalResourceId.of("ses_"+ strftime("%Y%m%d%H%M%S"))      
        },
        policy=AwsCustomResourcePolicy.from_sdk_calls(
            resources=["*"])
        )

        # Outputting the name of the bucket created
        CfnOutput(self, "businessq-data-bucket-name", value=self.data_bucket.bucket_name)
