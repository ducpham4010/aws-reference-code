import json
import boto3
import urllib3

s3 = boto3.client('s3')

print('---------------------------------------------')
print('S3 bucket list : ')
buckets = s3.list_buckets()
for bucket in buckets['Buckets']:
    print(bucket.get('Name'))

file = 'D:\\Tailieu\\fss\\python\\aws\\s3\\IFTB_BRANCH.csv'
with open(file, 'rb') as data:
    s3.upload_fileobj(data, 'ducph-bucket', 'temp\IFTB_BRANCH.csv')



def lambda_handler(event, context):
    # TODO implement
    print('---------------------------------------------')
    print('S3 bucket list : ')
    buckets = s3.list_buckets()
    for bucket in buckets['Buckets']:
        print(bucket.get('Name'))
        
    print('---------------------------------------------')
    print("File in ducph-bucket :")    
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('ducph-bucket')
    result = bucket.meta.client.list_objects(Bucket=bucket.name, Delimiter = '/')
    for item in result.get('Contents'):
        print(item.get('Key'))
        
        
    print('---------------------------------------------')  
    print('Dowload and upload to s3 from URL')
    http = urllib3.PoolManager()
    response = http.request('GET', 'https://st3.depositphotos.com/33200684/37530/i/1600/depositphotos_375305386-stock-photo-amazon-web-services-logo-on.jpg', preload_content = False)
    s3.upload_fileobj(response, 'ducph-bucket', 'aws.jpg')
    
    print('---------------------------------------------')  
    print('Delete object')
    s3.delete_object(Bucket = 'ducph-bucket', Key = 'aws.jpg')