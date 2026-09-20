FROM python:3.12-slim

LABEL maintainer="juan"
LABEL description="MeshCore -> WDGWars feeder (passive TCP listener via duplexer)"

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY feeder.py /app/feeder.py

ENTRYPOINT ["python3", "-u", "/app/feeder.py"]
