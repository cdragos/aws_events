from collections import namedtuple
from pathlib import Path
import json
import time

import boto3

import settings


Record = namedtuple('Record', ('file_name', 'source_bucket', 'sequence_number',))


def _download_file(filename, source_bucket, sequence_number):
    """Download a file from S3 to the settings assets folder."""
    s3 = boto3.resource('s3')
    settings.DOWNLOAD_PATH.mkdir(exist_ok=True)
    filepath = settings.DOWNLOAD_PATH / Path(filename).name
    s3.Bucket(source_bucket).download_file(filename, str(filepath))


def _get_kinessis_records(kinesis, shard_ids):
    """
    Process events from kinesis that have records with files that match
    `in/hydra/ninja-dev`.
    """
    for shard_id in shard_ids:
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
        _download_file(record.file_name,
                       record.source_bucket,
                       record.sequence_number)