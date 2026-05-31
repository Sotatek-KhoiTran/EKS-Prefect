import os
import time
from prefect import flow, get_run_logger


from pipelines.hooks.notification import MailNotification
from pipelines.hooks.state_logger import state_log_hook, log_flow_start
from pipelines.tasks.spark_tasks import (
    build_spark_application, submit_spark_job, 
    wait_for_spark_job, cleanup_spark_job, write_spark_application
)

mail_notifier = MailNotification()

@flow(
    name="prefect-spark-s3-etl", 
    on_running=[state_log_hook],
    on_failure=[state_log_hook, mail_notifier.send_noti],
    on_completion=[state_log_hook]
)
def etl_pipeline(
    job_name: str,
    dq_job_name: str,
    spark_namespace: str,
    log_db: str,
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
    log_flow_start()

    logger = get_run_logger()
    
    s3_script_bucket = s3_script_bucket or os.getenv("SCRIPT_BUCKET", "")
    spark_job_key = spark_job_key or os.getenv("SPARK_JOB_KEY", "")
    spark_dq_checks_key = spark_dq_checks_key or os.getenv("SPARK_DQ_CHECKS_KEY", "")
    spark_image = spark_image or os.getenv("SPARK_IMAGE", "")
    spark_application_output_file = spark_application_output_file or os.getenv("SPARK_APPLICATION_OUTPUT_FILE", ".generated/spark-application.yaml")

    job_app_name = f"{job_name.replace('_', '-')}-{int(time.time())}"
    dq_app_name = f"{dq_job_name.replace('_', '-')}-{int(time.time())}"
    
    # 3. Build Manifests
    spark_job_application = build_spark_application(job_name, job_app_name, s3_script_bucket, spark_job_key, spark_namespace, spark_image)
    spark_dq_application = build_spark_application(dq_job_name, dq_app_name, s3_script_bucket, spark_dq_checks_key, spark_namespace, spark_image)
    write_spark_application(spark_job_application, spark_application_output_file)

    succeeded = False
    job_submitted_name = dq_submitted_name = None
    
    try:
        job_submitted_name = submit_spark_job(spark_job_application)
        wait_for_spark_job(job_submitted_name, spark_namespace, poll_seconds, timeout_seconds)
        
        dq_submitted_name = submit_spark_job(spark_dq_application)
        wait_for_spark_job(dq_submitted_name, spark_namespace, poll_seconds, timeout_seconds)

        succeeded = True
    except Exception as exc:
        logger.error("Spark ETL job %s failed: %s", job_app_name, exc)
        raise
    finally:
        if cleanup and (succeeded or cleanup_on_failure):
            if job_submitted_name: cleanup_spark_job(job_submitted_name, spark_namespace)
            if dq_submitted_name: cleanup_spark_job(dq_submitted_name, spark_namespace)

if __name__ == "__main__":
    etl_pipeline()