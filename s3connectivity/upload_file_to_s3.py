import boto3
from botocore.exceptions import NoCredentialsError
bucket_name = 'krux-partners'
ACCESS_KEY = 'AKIAI4E2T6TSF5IAZ3GA'
SECRET_KEY = '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'
#path_mails = 'client-hotels/uploads/consent-data/' + str(date2) + '/' + md5_sf_gz

path_mails='client-hotels/uploads/cas/2020-03-15/test.dat'
def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        return True
    except Exception:
        return False

uploaded = upload_to_aws('/home/hadoop/test.dat', bucket_name, path_mails)