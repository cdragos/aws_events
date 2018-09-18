FROM python:3

RUN mkdir /code
WORKDIR /code

ADD ./requirements.txt /code/
ADD . /code/

RUN pip install -r requirements.txt

RUN mkdir ~/.aws
RUN chmod 700 ~/.aws
VOLUME ["~/.aws"]
