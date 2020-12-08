#!/usr/bin/env python3

import json
import logging
import boto3
from botocore.exceptions import ClientError
import os
import traceback
import pprint
from collections import namedtuple
import csv
from PyPDF2 import PdfFileReader
from io import BytesIO

## initialize logging
logger = logging.getLogger()
formatter = logging.Formatter(
    '[%(asctime)s)] %(filename)s:%(lineno)d} %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(formatter)


## services
S3_CLIENT = boto3.client('s3')
DB_CLIENT = boto3.resource('dynamodb')
STS_CLIENT = boto3.client('sts')

edbroleArn = 'arn:aws:iam::306023822385:role/regquest_xaccount_s3_role'
response = STS_CLIENT.assume_role(
    RoleArn=edbroleArn, RoleSessionName="test_edb")
credentials = response['Credentials']

client = boto3.client('s3',
                      aws_access_key_id=credentials['AccessKeyId'],
                      aws_secret_access_key=credentials['SecretAccessKey'],
                      aws_session_token=credentials['SessionToken'])

s3_resource = boto3.resource('s3', aws_access_key_id=credentials['AccessKeyId'],
                             aws_secret_access_key=credentials['SecretAccessKey'],
                             aws_session_token=credentials['SessionToken'])


def handler(event, context):

    ## read delta file
    #delta_files = ['mdit/fda/delta/2020/10/06/part-00000-e38d7780-b542-4c30-933d-b574e8bbbd5f-c000.csv','mdit/fda/delta/2020/10/07/part-00000-b0981bb1-d00b-4900-8fc6-1fbaea4b2724-c000.csv',
    #'mdit/fda/delta/2020/10/14/part-00000-5e1cd7c0-a0e4-4c14-ad00-c381811fedf2-c000.csv','mdit/fda/delta/2020/10/21/part-00000-a8bd6ca5-f9a7-409b-bd2b-6964c1397be1-c000.csv']
    delta_files = [
        'mdit/fda/delta/2020/10/29/part-00000-be415fdb-ee07-4cb7-95ec-e67e53ec2e8d-c000.csv']
    data = []
    for path in delta_files:
        read_delta_file(path)

    logging.info(f"Total number of records:{len(data)}")
    filename = 'delta_10_29.csv'
    write_to_s3(data, filename)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def read_delta_file(delta_file_path):
    edb_bucket = os.getenv('EDP_BUCKET')

    total_pages = 0

    try:
        response = client.get_object(Bucket=edb_bucket, Key=delta_file_path)
        content = response['Body'].read().decode('utf-8')

        lines = content.split("\n")
        print(f"length of lines:{len(lines)}")
        return
        csv_reader = csv.DictReader((line for line in lines if not line.isspace(
        ) and not line.replace(",", "").isspace()), delimiter=',')

        def map_row(row): return {
            'appplication_docs_type_id': row['applicationdocstypeid'],
            'application_no': row['applno'],
            'submission_type': row['submissiontype'],
            'submission_no': row['submissionno'],
            'application_docs_url': row['url'],
            'drug_name': row['drugname'],
            's3_path': row['path'],
            'url': row['url'],
            'filename': row['file_name']
        }
        all_records = []
        for row in list(csv_reader):
            s3_path = row['path']

            ## cfm folder her inner pdfs
            if row['path'].endswith("cfm"):

                bucket_name, key, filename = split_s3_url(s3_path)
                inner_pdf_paths = get_s3_objects(
                    bucket_name, prefix=key, suffix=".pdf")
                for path in list(inner_pdf_paths):
                    new_row = row.copy()
                    new_row = map_row(new_row)
                    new_row['path'] = path
                    new_row['s3_path'] = make_s3_uri(edb_bucket, path)

                    num_pages = get_num_pages(path)
                    new_row['num_pages'] = num_pages
                    all_records.append(new_row)
                    total_pages += int(num_pages) if num_pages is not None else 0
            else:
                new_row = map_row(row)
                num_pages = get_num_pages(row['path'])
                new_row['num_pages'] = num_pages
                total_pages += int(num_pages) if num_pages is not None else 0
                all_records.append(new_row)

        print(f"Total number of pages:{total_pages}")
        return all_records

    except ClientError as e:
        print(e)
        logger.exception(
            f"failed to read contents of the file: {delta_file_path}")


def get_num_pages(s3_path):
    (bucket_name, prefix, filename) = split_s3_url(s3_path)

    edb_bucket = os.getenv('EDP_BUCKET')
    if "mdit" not in prefix:
        prefix = "mdit/" + prefix

    try:
        response = client.get_object(Bucket=edb_bucket, Key=prefix)
        content = response['Body'].read()
        pdfFile = PdfFileReader(BytesIO(content))
        num_pages = pdfFile.getNumPages()

        return num_pages

    except Exception as ex:
        logger.exception("{}".format(ex))
        return 0


def make_s3_uri(bucket_name, key):
    return 's3://' + bucket_name + "/" + key


def split_s3_url(s3_object_url):
    """Method to slit s3 url into its component parts

    Args:
        s3_object_url: s3 url

    Returns:
        bucket_name: s3 bucket name
        prefix: s3 url prefix
        filename: s3 url filename

    """
    # remove s3://

    s3_object_url = s3_object_url.replace(
        "s3://", "") if "s3://" in s3_object_url else s3_object_url

    s3_parts = s3_object_url.split("/")
    bucket_name = s3_parts[0]

    prefix = "/".join(s3_parts[1:])
    filename = os.path.basename(prefix)

    logger.debug("%s bucket name: %s", s3_object_url, bucket_name)
    logger.debug("%s prefix: %s", s3_object_url, prefix)
    logger.debug("%s filename: %s", s3_object_url, filename)

    return (bucket_name, prefix, filename)


def get_s3_objects(bucket, prefix='', suffix='.csv'):
    """[summary]

    Args:
        bucket ([type]): [description]
        prefix (str, optional): [description]. Defaults to ''.
    """

    kwargs = {'Bucket': bucket, 'Prefix': prefix}

    while True:
        response = client.list_objects_v2(**kwargs)
        print(response)
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith(suffix):
                    yield key
            try:
                kwargs['ContinuationToken'] = response['NextContinuationToken']
            except KeyError:
                break
        else:
            break


def write_to_s3(all_records, filename):
    gra_bucket_name = 'lly-regquest-sagemaker-dev'
    prefix = f'miscellaneous/{filename}'
    s3_client = boto3.client('s3')

    filename = f'/tmp/{filename}'

    with open(filename, mode='w', newline="") as csv_file:
        delta_writer = csv.writer(csv_file)
        delta_writer.writerow(['appplication_docs_type_id', 'application_no', 'submission_type', 'submission_no',
                               'application_docs_url', 'drug_name', 's3_path', 'url', 'filename', 'num_pages'])

        for record in all_records:
            row = [record['appplication_docs_type_id'], record['application_no'], record['submission_type'], record['submission_no'],
                   record['application_docs_url'], record['drug_name'], record['s3_path'], record['url'], record['filename'], record['num_pages']]
            print(row)
            delta_writer.writerow(row)

    s3_client.upload_file(filename, gra_bucket_name, prefix)
    logging.info("saved csv file")
