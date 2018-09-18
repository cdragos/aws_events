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
    """
    Get metadata information for a file and call an external tracking serivce.

    Args:
        filepath (Path): Filepath for the file that we track.
        filename (str): Filename.
        source_bucket (str): Amazon source bucket where the file is stored.
    """
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


def _save_sequence(sequence_number):
    """
    Save the last processed sequence number to a file. We are going to use it
    when the script runs again to start from the last processed event.

    Args:
        sequence_number (str): Sequence number for the event that was processed.
    """
    logger.info('Saving sequence_number={}'.format(sequence_number))
    filepath = settings.DOWNLOAD_PATH / 'sequence_number.txt'
    with filepath.open('w+') as f:
        f.write(sequence_number)


def _get_last_sequence():
    """
    Get the sequence number for the last event that was processed.

    Returns:
        str: Last sequence number that was processed.
    """
    filepath = settings.DOWNLOAD_PATH / 'sequence_number.txt'
    if not filepath.exists():
        return None

    with filepath.open('r') as f:
        sequence_number = f.read()

    return sequence_number


def _download_file(filename, source_bucket):
    """
    Download a file from S3 to the settings assets folder and extract it.

    Args:
        filename (str): Filename.
        source_bucket (str): Amazon source bucket where the file is stored.

    Returns:
        Path: Output path for the decompressed contents of the downloaded file.
    """
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


def _get_kinessis_records(kinesis, shard_ids, last_sequence=None):
    """
    Process events from kinesis that have records with files that match
    `in/hydra/ninja-dev`.

    Args:
        kinesis (): Amazon Kinesis client.
        shard_ids (list): List of kinesis shard ids.
        last_sequence (str): Last sequence number that was processed.

    Yields:
        Record: Event record that consist of filename, source_bucket
                and sequence number.
    """
    for shard_id in shard_ids:
        logger.info(
            'Geting events for shard with id={} and last_sequence_number={}'
            .format(shard_id, last_sequence or 'latest'))
        iter_data = {
            'StreamName': settings.KINESIS_STREAM,
            'ShardId': shard_id,
        }
        if last_sequence:
            iter_data['ShardIteratorType'] = 'AFTER_SEQUENCE_NUMBER'
            iter_data['StartingSequenceNumber'] = last_sequence
        else:
            iter_data['ShardIteratorType'] = 'LATEST'

        shard_iterator = kinesis.get_shard_iterator(**iter_data)['ShardIterator']
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

    # fetch the events from the last successful processed event
    last_sequence = _get_last_sequence()

    for record in _get_kinessis_records(kinesis, shard_ids, last_sequence):
        filepath_output = _download_file(
            record.filename, record.source_bucket)
        _save_sequence(record.sequence_number)
        _track_file(filepath_output, record.filename, record.source_bucket)


if __name__ == '__main__':
    fetch_events()
