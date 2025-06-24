FROM python:3.12.9

WORKDIR /app

ADD requirements.txt /app/

RUN pip install --no-cache-dir -r /app/requirements.txt

ADD *.py /app/
