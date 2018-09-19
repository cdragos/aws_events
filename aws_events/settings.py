from pathlib import Path
import os


KINESIS_STREAM = os.environ.get('KINESIS_STREAM', 'olxgroup-reservoir-recruit')
KINESIS_RECORDS_LIMIT = os.environ.get('KINESIS_RECORDS_LIMIT', 1000)

if 'DOWNLOAD_PATH' in os.environ:
    DOWNLOAD_PATH = Path(os.environ['DOWNLOAD_PATH'])
else:
    DOWNLOAD_PATH = Path(__file__).parent / 'assets'

if 'RULES_PATH' in os.environ:
    RULES_PATH = Path(os.environ['RULES_PATH'])
else:
    RULES_PATH = Path(__file__).parent / 'rules.yml'

TRACK_FILE_URL = 'https://tracking-dev.onap.io/h/bdyt-case-ex1-dc'
