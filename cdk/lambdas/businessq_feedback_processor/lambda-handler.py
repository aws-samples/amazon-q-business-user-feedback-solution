import boto3
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Business Q client
client = boto3.client('qbusiness')

# S3 client
s3 = boto3.client('s3')

# Bedrock client used to interact with APIs around models
bedrock = boto3.client(
    service_name='bedrock', 
    region_name='us-east-1'
)
    
# Bedrock Runtime client used to invoke and question the models
bedrock_runtime = boto3.client(
    service_name='bedrock-runtime', 
    region_name='us-east-1'
)

# SES for report sending
ses = boto3.client('ses')

# sink the feedback json to the s3 bucket
s3_bucket = os.environ['S3_DATA_BUCKET']

model_id = os.environ['MODELID']


def invoke_claude(model_id, prompt_context, prompt):
    """
    Invokes the Anthropic Claude 2 model to run an inference using the input
    provided in the request body.

    :param prompt: The prompt that you want Claude to complete.
    :return: Inference response from the model.
    """

    try:
        # The different model providers have individual request and response formats.
        # For the format, ranges, and default values for Anthropic Claude, refer to:
        # https://docs.anthropic.com/claude/reference/complete_post


        prompt = "<context>" + prompt_context + "</context>\n\n" + prompt

        # Claude requires you to enclose the prompt as follows:
        
        enclosed_prompt = "Human: " + prompt + "\n\nAssistant:"

        body = {
            "prompt": enclosed_prompt,
            "max_tokens_to_sample": 1000,
            "temperature": 0.5,
            "stop_sequences": ["\n\nHuman:"],
        }

        response = bedrock_runtime.invoke_model(
            modelId="anthropic.claude-v2", body=json.dumps(body)
        )

        logger.info(prompt)
        
        response_body = json.loads(response["body"].read())
        completion = response_body["completion"]

        return completion

    except ClientError:
        logger.error("Couldn't invoke Anthropic Claude")
        raise

def send_email(report_response):
    
    from_address = os.environ['FROM_ADDRESS']
    to_address = os.environ['TO_ADDRESS']
    
    response = ses.send_email(
        Source=from_address,
        Destination={
            'ToAddresses': [to_address]
        },
        Message={
            'Subject': {
                'Data': 'Business Q Feedback Report'
            },
            'Body': {
                'Text': {
                    'Data': report_response
                }
            }
        }
    )
    
    return("Email sent with message ID: " + response['MessageId'])
    
    
    
def get_previous_body(data: List[Dict[str, any]], target_message_id: str) -> Optional[str]:
    previous_body = None

    for item in data:
        if item.get('messageId') == target_message_id:
            break
        previous_body = item.get('body')

    return previous_body
    
def get_previous_source_attribution(data: List[Dict[str, any]], target_message_id: str) -> Optional[str]:
    previous_body = None

    for item in data:
        if item.get('messageId') == target_message_id:
            break
        previous_source_attribution = item.get('sourceAttribution')

    return previous_source_attribution

def extract_urls_from_json(data):
    urls = []
    try:
        data = json.loads(data)
        for item in data:
            urls.append(item['url'])
    except Exception as e:
        print(f"An error occurred: {e}")
    return urls
    

def lambda_handler(event, context):
    
    messageId = str(event["detail"]["requestParameters"]["messageId"])
    applicationId = event["detail"]["requestParameters"]["applicationId"]
    usefulness = event["detail"]["requestParameters"]["messageUsefulness"]['usefulness']
    submittedAt = event["detail"]["requestParameters"]["messageUsefulness"]['submittedAt']
    userId = event["detail"]["requestParameters"]['userId']

    response = client.list_messages(
        applicationId=event["detail"]["requestParameters"]["applicationId"],
        conversationId=event["detail"]["requestParameters"]["conversationId"],
        maxResults=10,
        userId=event["detail"]["requestParameters"]['userId']
    )
    
    # with thumbs down there are comments sometimes
    try:
        usefulness_comment = event["detail"]["requestParameters"]["messageUsefulness"]['comment']
    except KeyError:
        usefulness_comment = ""

    messages_list = []
    source_attribution_urls = []
    
    logger.info("All Messages")
    logger.info(response['messages'])
    response_data = ""
    sourceAttribution = ""
    
    # Loop through each message
    for message in response['messages']:
        
        if str(message['messageId']) == messageId:
            
            message_ID = str(message['messageId'])
            message_body = message['body']
            
            # get the message before to get the AI response
            previous_body = get_previous_body(response['messages'], message_ID)
            previous_body_source_attribution = get_previous_source_attribution(response['messages'], message_ID)
            logger.info(json.dumps(previous_body_source_attribution[0]))
            
            # get the sourceattribute urls from citations add tot a list
            source_attribution_urls = extract_urls_from_json(json.dumps(previous_body_source_attribution))
            logger.info(source_attribution_urls)

            # Add message details to the analytics data
            messages_list.append({
                'messageId': message_ID,
                'query': message_body,
                'message': previous_body,
                'source_attribution_urls': source_attribution_urls,
                'sourceAttribution': previous_body_source_attribution,
                'applicationId': applicationId,
                'usefulness': usefulness,
                'usefulness_comment':   usefulness_comment,
                'userId': userId,
                'submittedAt': submittedAt
            })
            
            # create json response payload
            response_data = json.dumps(messages_list[0])
            logger.info(response_data)
            
            current_date = datetime.now()
            key = f'feedback/year={current_date.year}/month={current_date.strftime("%m")}/day={current_date.strftime("%d")}/{message_ID}.json'
            
            bucket_name = s3_bucket
            s3.put_object(Body=response_data, Bucket=bucket_name, Key=key)
            
            prompt = '''

            You are an intelligent LLM prompt engineer. Write a content report for this RAG Chat user report. This report is important for my career. 

            List of userid, submittedAt, usefulness reason, and usefulness_comment, query, message, list all titles, snippet summary, and source_attribution_urls, sourceAttribution.

            List the key suggestion for improving the source content based on the usefulness_comment.

            In the Recommendations section, suggest 2-3 specific examples of content that can be added to the content sources to be more useful/appropriate based on the usefulness_comment.

            Do NOT use wiki syntax in the output.

            '''
            
            report_response = invoke_claude(model_id,response_data,prompt)
            logger.info(report_response)
            send_email(report_response)
            
            break
            
    # Return the JSON response
    return {
        'statusCode': 200,
        'body': response_data
    }