FROM prefecthq/prefect:3-python3.11

WORKDIR /opt/prefect

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flows ./flows
COPY spark-job.yaml ./spark-job.yaml

ENV SPARK_APPLICATION_FILE=/opt/prefect/spark-job.yaml
