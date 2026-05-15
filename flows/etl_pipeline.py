import os
import time
from pathlib import Path
from typing import Any

import yaml
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from prefect import flow, get_run_logger, task
from prefect.events import emit_event


SPARK_GROUP = "sparkoperator.k8s.io"
SPARK_VERSION = "v1beta2"
SPARK_PLURAL = "sparkapplications"
TERMINAL_STATES = {"COMPLETED", "FAILED", "FAILING", "UNKNOWN"}
SPARK_ETL_TRIGGER_EVENT = "prefect-spark-eks.raw-data.ready"
SPARK_ETL_TRIGGER_RESOURCE_ID = "prefect-spark-eks.raw-data"


def _load_kubernetes_config() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def _manifest_path() -> Path:
    configured_path = os.getenv("SPARK_APPLICATION_FILE", "/opt/prefect/spark-job.yaml")
    path = Path(configured_path)
    if path.exists():
        return path

    repo_path = Path(__file__).resolve().parents[1] / "spark-job.yaml"
    if repo_path.exists():
        return repo_path

    return Path("spark-job.yaml")


def _deep_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return current if current is not None else default


def _summarize_failure(application: dict[str, Any]) -> str:
    status = application.get("status", {})
    application_state = status.get("applicationState", {})
    driver_info = status.get("driverInfo", {})
    executor_state = status.get("executorState", {})

    details = {
        "applicationState": application_state,
        "driverInfo": driver_info,
        "executorState": executor_state,
        "submissionAttempts": status.get("submissionAttempts"),
        "terminationTime": status.get("terminationTime"),
    }
    return yaml.safe_dump(details, default_flow_style=False, sort_keys=True)


@task
def build_spark_application(
    spark_app_name: str,
    s3_bucket: str,
    spark_job_key: str,
    input_prefix: str,
    output_prefix: str,
    spark_namespace: str,
    spark_image: str,
) -> dict[str, Any]:
    with _manifest_path().open("r", encoding="utf-8") as file:
        manifest = yaml.safe_load(file)

    manifest["metadata"]["name"] = spark_app_name
    manifest["metadata"]["namespace"] = spark_namespace
    manifest["spec"]["image"] = spark_image
    manifest["spec"]["mainApplicationFile"] = f"s3a://{s3_bucket}/{spark_job_key}"
    manifest["spec"]["arguments"] = [
        "--input",
        f"s3a://{s3_bucket}/{input_prefix.rstrip('/')}/",
        "--output",
        f"s3a://{s3_bucket}/{output_prefix.rstrip('/')}/{spark_app_name}",
    ]
    manifest["spec"].setdefault("sparkConf", {}).update(
        {
            "spark.jars.ivy": "/tmp/.ivy2",
            "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2",
            "spark.executor.extraJavaOptions": "-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2",
        }
    )
    manifest["spec"]["driver"]["serviceAccount"] = "spark-driver-sa"
    return manifest


@task
def submit_spark_job(manifest: dict[str, Any]) -> str:
    logger = get_run_logger()
    _load_kubernetes_config()
    api = client.CustomObjectsApi()

    namespace = manifest["metadata"]["namespace"]
    name = manifest["metadata"]["name"]

    try:
        api.create_namespaced_custom_object(
            group=SPARK_GROUP,
            version=SPARK_VERSION,
            namespace=namespace,
            plural=SPARK_PLURAL,
            body=manifest,
        )
        logger.info("Created SparkApplication %s/%s", namespace, name)
    except ApiException as exc:
        if exc.status != 409:
            raise
        logger.info("SparkApplication %s/%s already exists; patching it", namespace, name)
        api.patch_namespaced_custom_object(
            group=SPARK_GROUP,
            version=SPARK_VERSION,
            namespace=namespace,
            plural=SPARK_PLURAL,
            name=name,
            body=manifest,
        )

    return name


@task
def wait_for_spark_job(spark_app_name: str, spark_namespace: str, poll_seconds: int, timeout_seconds: int) -> str:
    logger = get_run_logger()
    _load_kubernetes_config()
    api = client.CustomObjectsApi()

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        application = api.get_namespaced_custom_object(
            group=SPARK_GROUP,
            version=SPARK_VERSION,
            namespace=spark_namespace,
            plural=SPARK_PLURAL,
            name=spark_app_name,
        )
        state = _deep_get(application, "status", "applicationState", "state", default="SUBMITTED")
        logger.info("SparkApplication %s/%s state=%s", spark_namespace, spark_app_name, state)

        if state in TERMINAL_STATES:
            if state != "COMPLETED":
                failure_summary = _summarize_failure(application)
                logger.error(
                    "SparkApplication %s/%s failed with status:\n%s",
                    spark_namespace,
                    spark_app_name,
                    failure_summary,
                )
                raise RuntimeError(
                    f"SparkApplication {spark_namespace}/{spark_app_name} finished with state={state}. "
                    f"Inspect it with: kubectl describe sparkapplication {spark_app_name} -n {spark_namespace}"
                )
            return state

        time.sleep(poll_seconds)

    raise TimeoutError(f"Timed out waiting for SparkApplication {spark_namespace}/{spark_app_name}")


@task
def cleanup_spark_job(spark_app_name: str, spark_namespace: str) -> None:
    logger = get_run_logger()
    _load_kubernetes_config()
    api = client.CustomObjectsApi()

    try:
        api.delete_namespaced_custom_object(
            group=SPARK_GROUP,
            version=SPARK_VERSION,
            namespace=spark_namespace,
            plural=SPARK_PLURAL,
            name=spark_app_name,
        )
        logger.info("Deleted SparkApplication %s/%s", spark_namespace, spark_app_name)
    except ApiException as exc:
        if exc.status != 404:
            raise
        logger.info("SparkApplication %s/%s is already deleted", spark_namespace, spark_app_name)


@flow(name="prefect-spark-s3-etl")
def etl_pipeline(
    s3_bucket: str = os.getenv("DATA_BUCKET", "<YOUR_BUCKET_NAME>"),
    spark_job_key: str = os.getenv("SPARK_JOB_KEY", "jobs/etl_job.py"),
    input_prefix: str = os.getenv("SPARK_INPUT_PREFIX", "raw"),
    output_prefix: str = os.getenv("SPARK_OUTPUT_PREFIX", "processed"),
    spark_namespace: str = os.getenv("SPARK_NAMESPACE", "spark-jobs"),
    spark_image: str = os.getenv("SPARK_IMAGE", "docker.io/library/spark:3.5.1-python3"),
    cleanup: bool = True,
    cleanup_on_failure: bool = False,
    poll_seconds: int = 15,
    timeout_seconds: int = 1800,
) -> str:
    spark_app_name = f"etl-spark-job-{int(time.time())}"
    manifest = build_spark_application(
        spark_app_name=spark_app_name,
        s3_bucket=s3_bucket,
        spark_job_key=spark_job_key,
        input_prefix=input_prefix,
        output_prefix=output_prefix,
        spark_namespace=spark_namespace,
        spark_image=spark_image,
    )
    submitted_name = submit_spark_job(manifest)

    succeeded = False
    try:
        result = wait_for_spark_job(
            spark_app_name=submitted_name,
            spark_namespace=spark_namespace,
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
        )
        succeeded = True
        return result
    finally:
        if cleanup and (succeeded or cleanup_on_failure):
            cleanup_spark_job(submitted_name, spark_namespace)


@flow(name="emit-spark-etl-requested-event")
def emit_spark_etl_requested_event(
    s3_bucket: str = os.getenv("DATA_BUCKET", "<YOUR_BUCKET_NAME>"),
    spark_job_key: str = os.getenv("SPARK_JOB_KEY", "jobs/etl_job.py"),
    input_prefix: str = os.getenv("SPARK_INPUT_PREFIX", "raw"),
    output_prefix: str = os.getenv("SPARK_OUTPUT_PREFIX", "processed"),
    spark_namespace: str = os.getenv("SPARK_NAMESPACE", "spark-jobs"),
    spark_image: str = os.getenv("SPARK_IMAGE", "docker.io/library/spark:3.5.1-python3"),
) -> str:
    logger = get_run_logger()
    payload = {
        "s3_bucket": s3_bucket,
        "spark_job_key": spark_job_key,
        "input_prefix": input_prefix,
        "output_prefix": output_prefix,
        "spark_namespace": spark_namespace,
        "spark_image": spark_image,
    }
    emitted_event = emit_event(
        event=SPARK_ETL_TRIGGER_EVENT,
        resource={
            "prefect.resource.id": SPARK_ETL_TRIGGER_RESOURCE_ID,
            "prefect.resource.name": "Spark ETL raw data",
        },
        payload=payload,
    )
    logger.info(
        "Emitted %s for %s with payload=%s",
        SPARK_ETL_TRIGGER_EVENT,
        SPARK_ETL_TRIGGER_RESOURCE_ID,
        payload,
    )
    return str(getattr(emitted_event, "id", emitted_event))


if __name__ == "__main__":
    etl_pipeline()
