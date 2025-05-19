FROM python:3.12-slim

RUN apt-get update && apt-get install -y gcc libffi-dev && apt-get clean

WORKDIR /app
COPY requirements.txt .

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . /app

CMD ["flask", "run", "--host=0.0.0.0"]