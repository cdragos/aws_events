import os


KINESIS_STREAM = 'olxgroup-reservoir-recruit'
KINESIS_RECORDS_LIMIT = 1000


DOWNLOAD_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'assets')
