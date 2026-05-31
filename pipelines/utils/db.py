import json
import os

import boto3
import psycopg2


def get_db_connection():
    region_name = os.getenv("AWS_REGION", "ap-southeast-1")
    secret_id = os.getenv("PREFECT_DB_SECRET_ID", "prefect/postgres-credentials")

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)

    db_credentials = json.loads(response["SecretString"])

    return psycopg2.connect(
        host=db_credentials["host"],
        port=db_credentials["port"],
        dbname=db_credentials["dbname"],
        user=db_credentials["username"],
        password=db_credentials["password"],
    )
