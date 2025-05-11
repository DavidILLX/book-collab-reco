FROM python:3.9-slim

RUN apt-get update && apt-get install -y gcc libffi-dev && apt-get clean

WORKDIR /db_init
COPY requirements.txt .

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . /db_init

CMD [ "python", "basic_etl.py" ]