import boto3
import os
from os import listdir
from os.path import isfile, join
import datetime

current_date_run = str(datetime.datetime.now().date())

s3_client = boto3.client('s3')


def download_dir_to_local(prefix, local, bucket, client=s3_client):
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket': bucket,
        'Prefix': prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)

def upload_to_krux_bucket(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        s3_file=output_prefix + file
        local_file=local_file + file
        print("Uploading " + local_file + " to " + s3_file)
        s3.upload_file(local_file, bucket, s3_file)
        return True
    except Exception:
        return False

bucket_name = 'krux-partners'
ACCESS_KEY = 'AKIAI4E2T6TSF5IAZ3GA'
SECRET_KEY = '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'


output_prefix= 'client-hotels/uploads/cas/' + current_date_run + '/'
hcom_source_prefix = 'data/kruz/' + current_date_run + '/'
local_directory_to_download = '/tmp/hcom-data-lab-dds-customer-file-extract'
source_s3_bucket = 'hcom-data-lab-dds-customer-file-extract'
path_to_upload = '/tmp/hcom-data-lab-dds-customer-file-extract/data/kruz/' + current_date_run + '/'

download_dir_to_local(hcom_source_prefix, local_directory_to_download, source_s3_bucket)


files_in_dds_bucket = [f for f in listdir(path_to_upload) if isfile(join(path_to_upload, f))]


upload_result = []

for file in files_in_dds_bucket:
    if file != '_SUCCESS':
        uploaded = upload_to_krux_bucket(path_to_upload, bucket_name, file)
        upload_result.append(uploaded)
