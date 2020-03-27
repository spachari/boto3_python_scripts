from boto3.session import Session

bucket_name = 'krux-partners'
ACCESS_KEY = 'AKIAI4E2T6TSF5IAZ3GA'
SECRET_KEY = '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'

session = Session(aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY)
s3 = session.resource('s3')
your_bucket = s3.Bucket(bucket_name)

for s3_file in your_bucket.objects.all():
    print(s3_file.key)

