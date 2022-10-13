#!/usr/bin/python
# -*- coding: utf-8 -*-

'''To use gzip file between python application and S3 directly for Python3.
Python 2 version - https://gist.github.com/a-hisame/f90815f4fae695ad3f16cb48a81ec06e
'''

import io
import gzip
import json

import boto3


def upload_json_gz(s3client, bucket, key, obj, default=None, encoding='utf-8'):
    ''' upload python dict into s3 bucket with gzip archive '''
    inmem = io.BytesIO()
    with gzip.GzipFile(fileobj=inmem, mode='wb') as fh:
        with io.TextIOWrapper(fh, encoding=encoding) as wrapper:
            wrapper.write(json.dumps(obj, ensure_ascii=False, default=default))
    inmem.seek(0)
    s3client.put_object(Bucket=bucket, Body=inmem, Key=key)
    
def upload_json_gz(s3client, bucket, key, obj, default=None, encoding='utf-8'):
    ''' upload python dict into s3 bucket with gzip archive '''
    inmem = io.BytesIO()
    with gzip.GzipFile(fileobj=inmem, mode='wb') as fh:
        with io.TextIOWrapper(fh, encoding=encoding) as wrapper:
            wrapper.write(json.dumps(obj, ensure_ascii=False, default=default))
    inmem.seek(0)
    s3client.put_object(Bucket=bucket, Body=inmem, Key=key)


def download_json_gz(s3client, bucket, key):
    ''' download gzipped json file from s3 and convert to dict '''
    response = s3client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read()
    with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as fh:
        return json.load(fh)


if __name__ == '__main__':
    bucketname = ''      # input for your bucketname
    key = 'tmp.json.gz'  # input for your key on S3 (means S3 object fullpath)
    s3 = boto3.client('s3')
    upload_json_gz(s3, bucketname, key, {'number': 122, 'str': 'my_string', 'list': ['item1', 'item2']})
    actual = download_json_gz(s3, bucketname, key)
    assert actual == {'あ': 'いうえお'}
