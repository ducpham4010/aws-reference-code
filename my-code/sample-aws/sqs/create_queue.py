import boto3

# Create SQS client
sqs = boto3.client('sqs')

# Create a SQS queue
def Create_queue(queue_name):
    response = sqs.create_queue(
        QueueName= queue_name ,
        Attributes={
            'DelaySeconds': '60',
            'MessageRetentionPeriod': '86400'
        }
    )
    return response

print(Create_queue('SQS_QUEUE_NAME')['QueueUrl'])