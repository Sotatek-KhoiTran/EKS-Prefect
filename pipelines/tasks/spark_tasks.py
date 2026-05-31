import time
import yaml
from pathlib import Path
from typing import Any
from kubernetes import client
from kubernetes.client.rest import ApiException
from prefect import task, get_run_logger

from pipelines.utils.k8s import load_kubernetes_config 
from pipelines.utils.config import get_file_path, load_spark_job_config, deep_get, render_value

SPARK_GROUP = "sparkoperator.k8s.io"
SPARK_VERSION = "v1beta2"
SPARK_PLURAL = "sparkapplications"
TERMINAL_STATES = {"COMPLETED", "FAILED", "FAILING", "UNKNOWN"}

def _summarize_failure(application: dict[str, Any]) -> str:
    status = application.get("status", {})
    details = {
        "applicationState": status.get("applicationState", {}),
        "driverInfo": status.get("driverInfo", {}),
        "executorState": status.get("executorState", {}),
        "submissionAttempts": status.get("submissionAttempts"),
        "terminationTime": status.get("terminationTime"),
    }
    return yaml.safe_dump(details, default_flow_style=False, sort_keys=True)

@task
def build_spark_application(
    job_name: str,
    spark_app_name: str,
    s3_script_bucket: str,
    spark_job_key: str,
    spark_namespace: str,
    spark_image: str,
) -> dict[str, Any]:
    logger = get_run_logger()
    logger.info("Building SparkApplication for job_name=%s, spark_app_name=%s", job_name, spark_app_name)
    
    with get_file_path("SPARK_APPLICATION_FILE", "spark-job.yaml").open("r", encoding="utf-8") as file:
        manifest_template = yaml.safe_load(file)

    job_config = load_spark_job_config(job_name)
    
    resolved_namespace = spark_namespace or job_config.get("namespace", "spark-jobs")
    resolved_image = spark_image or job_config.get("image", "")
    main_application_file = spark_job_key or job_config.get(
        "main_application_file",
        f"spark/jobs/{job_name}.py",
    )
    if not str(main_application_file).startswith("s3a://"):
        main_application_file = f"s3a://{s3_script_bucket}/{str(main_application_file).lstrip('/')}"

    context = {
        "spark_app_name": spark_app_name,
        "spark_namespace": resolved_namespace,
        "spark_image": resolved_image,
        "main_application_file": main_application_file,
        "driver_cores": deep_get(job_config, "driver", "cores", default=1),
        "driver_core_limit": deep_get(job_config, "driver", "coreLimit", default="1200m"),
        "driver_memory": deep_get(job_config, "driver", "memory", default="1g"),
        "driver_service_account": deep_get(job_config, "driver", "serviceAccount", default="spark-driver-sa"),
        "executor_instances": deep_get(job_config, "executor", "instances", default=2),
        "executor_cores": deep_get(job_config, "executor", "cores", default=1),
        "executor_memory": deep_get(job_config, "executor", "memory", default="1g"),
    }
    
    manifest = render_value(manifest_template, context)
    
    manifest["spec"]["arguments"] = job_config.get("arguments", [])
    # manifest["spec"].setdefault("sparkConf", {}).update(
    #     {   
    #         "spark.kubernetes.authenticate.driver.serviceAccountName": context["driver_service_account"],
    #         "spark.jars.ivy": "/tmp/.ivy2",
    #         "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2",
    #         "spark.executor.extraJavaOptions": "-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2",
    #     }
    # )
    manifest["spec"]["driver"].update(job_config.get("driver", {}))
    manifest["spec"]["executor"].update(job_config.get("executor", {}))
    manifest["spec"]["driver"]["serviceAccount"] = context["driver_service_account"]
    
    logger.info("Successfully built SparkApplication manifest for job_name=%s, spark_app_name=%s", job_name, spark_app_name)
    return manifest

@task
def submit_spark_job(manifest: dict[str, Any]) -> str:
    logger = get_run_logger()
    load_kubernetes_config()
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
def write_spark_application(manifest: dict[str, Any], output_file: str) -> str:
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(manifest, file, default_flow_style=False, sort_keys=False)
    return str(output_path)

@task
def wait_for_spark_job(spark_app_name: str, spark_namespace: str, poll_seconds: int, timeout_seconds: int) -> str:
    logger = get_run_logger()
    load_kubernetes_config()
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
        state = deep_get(application, "status", "applicationState", "state", default="SUBMITTED")
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
    load_kubernetes_config()
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