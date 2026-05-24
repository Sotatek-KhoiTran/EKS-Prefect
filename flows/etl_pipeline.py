import os
import time
import smtplib
from pathlib import Path
from typing import Any

import yaml
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from prefect import flow, get_run_logger, task
from prefect.events import emit_event
from prefect.blocks.system import Secret


SPARK_GROUP = "sparkoperator.k8s.io"
SPARK_VERSION = "v1beta2"
SPARK_PLURAL = "sparkapplications"
TERMINAL_STATES = {"COMPLETED", "FAILED", "FAILING", "UNKNOWN"}
SPARK_ETL_TRIGGER_EVENT = "prefect-spark-eks.raw-data.ready"
SPARK_ETL_TRIGGER_RESOURCE_ID = "prefect-spark-eks.raw-data"
DEFAULT_SPARK_JOB_NAME = "etl_job_1"


def _is_unresolved_template(value: str | None) -> bool:
    return bool(value and value.strip().startswith("{{") and value.strip().endswith("}}"))


def _load_kubernetes_config() -> None:
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        try:
            config.load_incluster_config()
        except config.ConfigException as exc:
            raise RuntimeError(
                "Failed to load in-cluster Kubernetes config. "
                "The Prefect flow-run pod must run with serviceAccountName=prefect-flow-run "
                "and a mounted service account token."
            ) from exc
        _ensure_incluster_bearer_auth()
        return

    try:
        config.load_incluster_config()
        _ensure_incluster_bearer_auth()
    except config.ConfigException:
        config.load_kube_config()


def _ensure_incluster_bearer_auth() -> None:
    configuration = client.Configuration.get_default_copy()
    token = configuration.api_key.get("BearerToken") or configuration.api_key.get("authorization")
    if token and not configuration.auth_settings():
        if token.lower().startswith("bearer "):
            token = token.split(" ", 1)[1]
        configuration.api_key["BearerToken"] = token
        configuration.api_key_prefix["BearerToken"] = "Bearer"
        client.Configuration.set_default(configuration)


def _get_file_path(env_var: str, default_filename: str) -> Path:
    configured_path = os.getenv(env_var)
    if configured_path:
        path = Path(configured_path)
        if path.exists():
            return path

    repo_path = Path(__file__).resolve().parents[1] / default_filename
    if repo_path.exists():
        return repo_path

    return Path(default_filename)

class _SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _deep_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return current if current is not None else default


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _render_value(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format_map(_SafeFormatDict(context))
    if isinstance(value, list):
        return [_render_value(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render_value(item, context) for key, item in value.items()}
    return value


def _load_spark_job_config(job_name: str) -> dict[str, Any]:
    with _get_file_path("SPARK_JOB_CONFIG_FILE", "spark-job-config.yaml").open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    defaults = config.get("defaults", {})
    jobs = config.get("jobs", [])
    for job in jobs:
        if job.get("name") == job_name:
            return _deep_merge(defaults, job)

    available_jobs = ", ".join(job.get("name", "<missing-name>") for job in jobs)
    raise ValueError(f"Spark job config not found for job_name={job_name}. Available jobs: {available_jobs}")


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


def notify_mail(flow, flow_run, state):
    try:
        logger = get_run_logger()
    except Exception:
        logger = None

    sender_email = os.getenv("SENDER_EMAIL")
    receiver_email = os.getenv("RECEIVER_EMAIL")
    password = os.getenv("EMAIL_PASSWORD")

    if _is_unresolved_template(password):
        password = None

    if not password:
        try:
            password = Secret.load("gmail-app-password").get()
        except Exception as exc:
            message = f"Failed to load Prefect Secret block gmail-app-password: {exc}"
            if logger:
                logger.warning(message)
            else:
                print(message)
    
    if not all([sender_email, receiver_email, password]):
        missing = [
            name
            for name, value in {
                "SENDER_EMAIL": sender_email,
                "RECEIVER_EMAIL": receiver_email,
                "EMAIL_PASSWORD or gmail-app-password block": password,
            }.items()
            if not value
        ]
        message = f"Email credentials are not fully set. Missing: {', '.join(missing)}"
        if logger:
            logger.warning(message)
        else:
            print(message)
        return
    
    prefect_url = os.getenv(
        "PREFECT_UI_URL",
        "http://prefect-server:4200"
    )
    
    msg = MIMEMultipart()
    msg['From'] = f"Prefect Alerts <{sender_email}>"
    msg['To'] = receiver_email
    msg['Subject'] = f"Prefect Alert: Flow run '{flow_run.name}' failed with state {state.name}"

    body = f"""
        Your job {flow_run.name} entered {state.name}

        Message:
        {state.message}

        See the flow run in UI:
        {prefect_url}/flow-runs/flow-run/{flow_run.id}

        Tags: {flow_run.tags}

        Scheduled start: {flow_run.expected_start_time}
    """
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        if logger:
            logger.info("Sent failure notification email to %s", receiver_email)
    except Exception as e:
        message = f"Failed to send custom email: {e}"
        if logger:
            logger.error(message)
        else:
            print(message)
    return


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
    
    with _get_file_path("SPARK_APPLICATION_FILE", "spark-job.yaml").open("r", encoding="utf-8") as file:
        manifest_template = yaml.safe_load(file)

    job_config = _load_spark_job_config(job_name)
    
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
        "driver_cores": _deep_get(job_config, "driver", "cores", default=1),
        "driver_core_limit": _deep_get(job_config, "driver", "coreLimit", default="1200m"),
        "driver_memory": _deep_get(job_config, "driver", "memory", default="1g"),
        "driver_service_account": _deep_get(job_config, "driver", "serviceAccount", default="spark-driver-sa"),
        "executor_instances": _deep_get(job_config, "executor", "instances", default=2),
        "executor_cores": _deep_get(job_config, "executor", "cores", default=1),
        "executor_memory": _deep_get(job_config, "executor", "memory", default="1g"),
    }
    
    manifest = _render_value(manifest_template, context)
    
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
def write_spark_application(manifest: dict[str, Any], output_file: str) -> str:
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(manifest, file, default_flow_style=False, sort_keys=False)
    return str(output_path)


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


@flow(name="prefect-spark-s3-etl", on_failure=[notify_mail])
def etl_pipeline(
    job_name: str,
    dq_job_name: str,
    spark_namespace: str,
    cleanup: bool = True,
    cleanup_on_failure: bool = False,
    poll_seconds: int = 15,
    timeout_seconds: int = 1800,
    s3_script_bucket: str = "",
    spark_job_key: str = "",
    spark_dq_checks_key: str = "",
    spark_image: str = "",
    spark_application_output_file: str = "",
) -> str:
    s3_script_bucket = s3_script_bucket or os.getenv("SCRIPT_BUCKET", "")
    spark_job_key = spark_job_key or os.getenv("SPARK_JOB_KEY", "")
    spark_dq_checks_key = spark_dq_checks_key or os.getenv("SPARK_DQ_CHECKS_KEY", "")
    spark_image = spark_image or os.getenv("SPARK_IMAGE", "")
    spark_application_output_file = (
        spark_application_output_file
        or os.getenv("SPARK_APPLICATION_OUTPUT_FILE", ".generated/spark-application.yaml")
    )

    job_app_name_prefix = job_name.replace("_", "-")
    dq_app_name_prefix = dq_job_name.replace("_", "-")
    job_app_name = f"{job_app_name_prefix}-{int(time.time())}"
    dq_app_name = f"{dq_app_name_prefix}-{int(time.time())}"
    
    # spark_job_application = build_spark_application(
    #     job_name=job_name,
    #     spark_app_name=job_app_name,
    #     s3_script_bucket=s3_script_bucket,
    #     spark_job_key=spark_job_key,
    #     spark_namespace=spark_namespace,
    #     spark_image=spark_image,
    # )
    
    spark_dq_application = build_spark_application(
        job_name=dq_job_name,
        spark_app_name=dq_app_name,
        s3_script_bucket=s3_script_bucket,
        spark_job_key=spark_dq_checks_key,
        spark_namespace=spark_namespace,
        spark_image=spark_image,    
    )
    
    # write_spark_application(spark_job_application, spark_application_output_file)

    succeeded = False
    job_submitted_name = None
    dq_submitted_name = None
    try:
        # job_submitted_name = submit_spark_job(spark_job_application)
        # wait_for_spark_job(
        #     spark_app_name=job_submitted_name,
        #     spark_namespace=spark_namespace,
        #     poll_seconds=poll_seconds,
        #     timeout_seconds=timeout_seconds,
        # )
        
        dq_submitted_name = submit_spark_job(spark_dq_application)
        wait_for_spark_job(
            spark_app_name=dq_submitted_name,
            spark_namespace=spark_namespace,
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
        )

        succeeded = True
    except Exception as exc:
        logger = get_run_logger()
        logger.error("Spark ETL job %s/%s failed: %s", spark_namespace, job_app_name, exc)
        raise
    finally:
        if cleanup and (succeeded or cleanup_on_failure):
            cleanup_spark_job(job_submitted_name, spark_namespace)
            cleanup_spark_job(dq_submitted_name, spark_namespace)


@flow(name="emit-spark-etl-requested-event")
def emit_spark_etl_requested_event(
    job_name: str = os.getenv("SPARK_JOB_NAME", DEFAULT_SPARK_JOB_NAME),
    s3_bucket: str = os.getenv("DATA_BUCKET", "<YOUR_BUCKET_NAME>"),
    spark_job_key: str = os.getenv("SPARK_JOB_KEY", ""),
    input_prefix: str = os.getenv("SPARK_INPUT_PREFIX", ""),
    output_prefix: str = os.getenv("SPARK_OUTPUT_PREFIX", ""),
    spark_namespace: str = os.getenv("SPARK_NAMESPACE", "spark-jobs"),
    spark_image: str = "",
) -> str:
    logger = get_run_logger()
    spark_image = spark_image or os.getenv("SPARK_IMAGE", "docker.io/library/spark:3.5.1-python3")
    payload = {
        "job_name": job_name,
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
