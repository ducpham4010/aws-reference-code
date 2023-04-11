from ast import Expression
from http import client
import boto3

client = boto3.client('s3')

res = client.select_object_content(
    Bucket = 'ducph-bucket',
    Key = 'IFTB_BRANCH.csv',
    Expression = 'Select * from S3Object',
    ExpressionType = 'SQL',
    InputSerialization = {'CSV': {'FileHeaderInfo':'USE'}},
    OutputSerialization = {'CSV':{}}
)

print(res)

#for event in res['Payload']:
#    if 'Records' in event:
#        print(event['Records']['Payload'].decode())