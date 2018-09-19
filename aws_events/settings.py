from pathlib import Path


KINESIS_STREAM = 'olxgroup-reservoir-recruit'
KINESIS_RECORDS_LIMIT = 1000

DOWNLOAD_PATH = Path(__file__).parent / 'assets'
RULES_PATH = Path(__file__).parent / 'rules.yml'
