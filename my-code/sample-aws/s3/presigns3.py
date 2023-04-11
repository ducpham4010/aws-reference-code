# Tạo một thông tin truy cập tạm thời có quyền upload file lên s3 trong thời gian hữu hạn
import requests
import boto3

# Generate s3 presign url, require s3 pemission

s3Client = boto3.client('s3')

fileKey = 'IFTB_BRANCH.csv'
filepath = 'D:\\Tailieu\\fss\\python\\aws\\s3\\IFTB_BRANCH.csv'
res = s3Client.generate_presigned_post(
    Bucket = 'ducph-bucket',
    Key = fileKey,
    ExpiresIn = 600 # second - thời gian thông tin truy cập tạm thời còn hiều lực
)

print(res)

fin = open(filepath, 'rb')
file = {'file':fin}

try:
    r = requests.post(res['url'], data = res['fields'], files=file)
    print(r.status_code)
finally:
    fin.close()