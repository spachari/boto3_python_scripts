import boto3
from boto3 import client

bucket_name = 'krux-partners'
ACCESS_KEY = 'AKIAI4E2T6TSF5IAZ3GA'
SECRET_KEY = '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'

prefix = "client-hotels/uploads/cas/2020-03-23/"

s3_conn = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)  # type: BaseClient  ## again assumes boto.cfg setup, assume AWS S3
s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

if 'Contents' not in s3_result:
    print(s3_result)
    #return []

file_list = []
for key in s3_result['Contents']:
    file_list.append(key['Key'])
print(f"List count = {len(file_list)}")

while s3_result['IsTruncated']:
    continuation_key = s3_result['NextContinuationToken']
    s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/",
                                        ContinuationToken=continuation_key)
    for key in s3_result['Contents']:
        file_list.append(key['Key'])
    print(f"List count = {len(file_list)}")
return file_list