import boto3
import json
import os
import zipfile
import shutil

s3 = boto3.resource('s3')

def copy(source_obj, dest_bucket, dest_key):
    print(f"copying from => {source_obj['Bucket']}/{source_obj['Key']} to => {dest_bucket}/{dest_key}")

    # copy object with tags
    s3.Object(dest_bucket, dest_key).copy_from(
        CopySource=source_obj,
        TaggingDirective='COPY')


def unzip(bucket:str, key:str, tmpdir:str):
    if not os.path.exists(tmpdir):
        os.makedirs(tmpdir)

    localzip_path = os.path.join(tmpdir, os.path.basename(key))
    s3.Object(bucket, key).download_file(localzip_path)

    localzip = zipfile.ZipFile(localzip_path)
    localzip.extractall(tmpdir)
    localzip.close()

    os.remove(localzip_path)


def upload_local_files(tmpdir:str, dest_bucket:str, dest_prefix:str):
    files = [ os.path.join(tmpdir, f) for f in os.listdir(tmpdir) if os.path.isfile(os.path.join(tmpdir, f)) ]
    print(f"files for upload => {files} with dest prefix => {dest_prefix}")

    for f in files:
        dest_key = os.path.join(dest_prefix, os.path.basename(f))
        print(f"preparing to upload local file => {f} to => {dest_key}")

        s3.Object(dest_bucket, dest_key).upload_file(f)

    
def main(event, context):
    # save event to logs
    print(json.dumps(event))

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    
    # make sure event is a file and not a folder
    if source_key.endswith('/'):
        print("key is a folder, skipping...")
        return

    # if scripts version = local, skip archive
    scripts_version = source_key.split('/')[1]
    print(f'scripts version => {scripts_version}')

    # strip off first 2 folders working/{scripts_version}/
    new_key_suffix = '/'.join(source_key.split('/')[2:])
    print(f"new key suffix => {new_key_suffix}")
    
    source_obj = { 'Bucket': bucket_name, 'Key': source_key }

    # if zip, unzip, then copy to zip
    if source_key.endswith('.zip'):
        tmpdir = '/tmp/'+os.path.splitext(os.path.basename(source_key))[0]
        
        unzip(bucket_name, source_key, tmpdir)

        # for zip file we don't want filename, so strip it off
        scripts_prefix = new_key_suffix.rsplit('/', 1)[0]

        upload_local_files(tmpdir, bucket_name, os.path.join('scripts',scripts_prefix))

        shutil.rmtree(tmpdir)
        
    else:
        # otherwise copy file to scripts (where etljobs point to)
        copy(source_obj, bucket_name, os.path.join('scripts', new_key_suffix))

    # copy file to archive (to quickly revert to old version)
    if not scripts_version.startswith('local-'):
        copy(source_obj, bucket_name, os.path.join('archive', scripts_version, new_key_suffix))