from collections import namedtuple
from pathlib import Path
import gzip
import hashlib
import json
import logging
import shutil
import time

import boto3
import requests

import settings


logger = logging.getLogger(__file__)
logging.basicConfig(level='INFO')


Record = namedtuple('Record', ('filename', 'source_bucket', 'sequence_number',))


def _track_file(filepath, filename, source_bucket):
    """Get metadata information for a file and call an external tracking serivce."""
    hash_md5 = hashlib.md5()
    num_of_lines = 0
    with filepath.open('r') as f:
        for line in f:
            hash_md5.update(line.encode('utf-8'))
            num_of_lines += 1

    url = 'https://tracking-dev.onap.io/h/bdyt-case-ex1-dragos-catarahia'
    params = {
        'file_name': filename,
        'source_bucket': source_bucket,
        'nlines': num_of_lines,
        'hash': hash_md5.hexdigest(),
    }
    logger.info('Track file metadata={}'.format(params))
    requests.get(url, params=params)


def _download_file(filename, source_bucket):
    """Download a file from S3 to the settings assets folder and extract it."""
    logger.info(
        'Started downloading file with filename={} from source_bucket={}'
        .format(filename, source_bucket))
    s3 = boto3.resource('s3')
    settings.DOWNLOAD_PATH.mkdir(exist_ok=True)
    filepath = settings.DOWNLOAD_PATH / Path(filename).name
    s3.Bucket(source_bucket).download_file(filename, str(filepath))

    filepath_output = (
        settings.DOWNLOAD_PATH / Path(filepath.stem).with_suffix('.json'))
    # extract contents from the gzip file to a new json file
    with gzip.open(str(filepath), 'rb') as f_in, \
         filepath_output.open('wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    # delete the gziped file
    filepath.unlink()

    return filepath_output


def _get_kinessis_records(kinesis, shard_ids):
    """
    Process events from kinesis that have records with files that match
    `in/hydra/ninja-dev`.
    """
    for shard_id in shard_ids:
        logger.info('Geting events for shard with id={}'.format(shard_id))
        shard_iterator = kinesis.get_shard_iterator(
            StreamName=settings.KINESIS_STREAM,
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )['ShardIterator']
        while True:
            response = kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=settings.KINESIS_RECORDS_LIMIT)
            shard_iterator = response['NextShardIterator']
            # wait between calls to avoid a `ProvisionedThroughputExceededException`
            time.sleep(0.2)

            # skip empty records
            if len(response['Records']) == 0:
                continue

            for record in response['Records']:
                sequence_number = record['SequenceNumber']
                data = json.loads(record['Data'])
                filename = data['file_name']
                source_bucket = data['source_bucket']

                if filename.startswith('in/hydra/ninja-dev/'):
                    yield Record(filename, source_bucket, sequence_number)


def fetch_events():
    """
    Fetch kinesis events, that have records with files that we download from S3,
    and call an external service to track file metadata.
    """
    kinesis = boto3.client('kinesis')
    stream = kinesis.describe_stream(StreamName=settings.KINESIS_STREAM)
    shard_ids = (
        shard['ShardId'] for shard in stream['StreamDescription']['Shards'])

    for record in _get_kinessis_records(kinesis, shard_ids):
        filepath_output = _download_file(
            record.filename, record.source_bucket)
        _track_file(filepath_output, record.filename, record.source_bucket)
