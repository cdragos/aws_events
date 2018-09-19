# AWS Events

Python script that reads records from Kinesis stream and process events based
on `rules.yml` configuration file.

The script downloads files from S3 in the `assets` folder in the project folder.

### Requirements

- Docker (https://www.docker.com)

### How to start

1. `$ docker-compose build`
2. `$ docker-compose run aws_events`
