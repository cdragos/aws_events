from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib3 import Retry
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
Record = namedtuple('Record', ('filename', 'source_bucket', 'sequence_number'))

TRACK_FILE_URL = 'https://tracking-dev.onap.io/h/bdyt-case-ex1-dc'
PROCESS_FILE_URL = 'https://tracking-dev.onap.io/h/bdyt-case-ex2-dc'


def track_url(url, params):
    """
    Send a GET request to an url with params.

    Args:
        url (str): Url.
        params (dict): GET request params for the url.

    Returns
        request.Response: Server response.
    """
    logger.info(
        'Track request initialized for url={} and params={}'
        .format(url, params))
    with requests.Session() as session:
        retries = Retry(total=3, backoff_factor=0.1)
        adapter = requests.adapters.HTTPAdapter(max_retries=retries)
        session.mount('https://', adapter)
        return requests.get(url, params)


def process_file(filepath, executor):
    """
    Process downloaded file

    Args:
        filepath (Path): Filepath for the file that we process.
    """
    logger.info('Process file with name={}'.format(filepath.name))

    with filepath.open('r') as f:
        for line in f:
            data = json.loads(line)
            if data['params']['en'] not in ('session_start',
                                            'location_change_click'):
                continue

            params = {
                'sl': data['params']['sl'],
                'sc': data['params']['sc'],
                'gsl': data['meta']['cross_domain_session_long'],
            }
            executor.submit(track_url, PROCESS_FILE_URL, params)


def track_file(filepath, filename, source_bucket, executor):
    """
    Get metadata information for a file and call an external tracking serivce.

    Args:
        filepath (Path): Filepath for the file that we track.
        filename (str): Filename.
        source_bucket (str): Amazon source bucket where the file is stored.
    """
    num_of_lines = 0
    with filepath.open('r') as f:
        for line in f:
            num_of_lines += 1

    with filepath.open('rb') as f:
        hash_md5 = hashlib.md5(f.read()).hexdigest()

    params = {
        'file_name': filename,
        'source_bucket': source_bucket,
        'nlines': num_of_lines,
        'hash': hash_md5,
    }
    logger.info('Track file metadata={}'.format(params))
    executor.submit(track_url, TRACK_FILE_URL, params)


def save_sequence(sequence_number):
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


def get_last_sequence():
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


def download_file(filename, source_bucket):
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


def get_kinessis_records(kinesis, shard_id, last_sequence=None):
    """
    Process events from kinesis that have records with files that match
    `in/hydra/ninja-dev`.

    Args:
        kinesis (botocore.client.Kinesis): Amazon Kinesis client.
        shard_ids (list): List of kinesis shard ids.
        last_sequence (str): Last sequence number that was processed.

    Yields:
        Record: Event record that consist of filename, source_bucket
                and sequence number.
    """
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
    shard_id = stream['StreamDescription']['Shards'][0]['ShardId']

    # fetch the events from the last successful processed event
    last_sequence = get_last_sequence()

    with ThreadPoolExecutor(max_workers=5) as executor:
        for record in get_kinessis_records(kinesis, shard_id, last_sequence):
            filepath = download_file(record.filename, record.source_bucket)
            save_sequence(record.sequence_number)
            track_file(filepath, record.filename, record.source_bucket, executor)
            process_file(filepath, executor)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    fetch_events()
