import os
import yaml
from pathlib import Path
from typing import Any

class SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"

def deep_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return current if current is not None else default

def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged

def render_value(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format_map(SafeFormatDict(context))
    if isinstance(value, list):
        return [render_value(item, context) for item in value]
    if isinstance(value, dict):
        return {key: render_value(item, context) for key, item in value.items()}
    return value

def get_file_path(env_var: str, default_filename: str) -> Path:
    configured_path = os.getenv(env_var)
    if configured_path:
        path = Path(configured_path)
        if path.exists():
            return path
        
    return Path(default_filename)

def load_spark_job_config(job_name: str) -> dict[str, Any]:
    with get_file_path("SPARK_JOB_CONFIG_FILE", "spark-job-config.yaml").open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}

    defaults = config.get("defaults", {})
    jobs = config.get("jobs", [])
    for job in jobs:
        if job.get("name") == job_name:
            return deep_merge(defaults, job)

    available_jobs = ", ".join(job.get("name", "<missing-name>") for job in jobs)
    raise ValueError(f"Spark job config not found for job_name={job_name}. Available jobs: {available_jobs}")