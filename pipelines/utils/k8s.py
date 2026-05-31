import os
from kubernetes import client, config

def _ensure_incluster_bearer_auth() -> None:
    configuration = client.Configuration.get_default_copy()
    token = configuration.api_key.get("BearerToken") or configuration.api_key.get("authorization")
    if token and not configuration.auth_settings():
        if token.lower().startswith("bearer "):
            token = token.split(" ", 1)[1]
        configuration.api_key["BearerToken"] = token
        configuration.api_key_prefix["BearerToken"] = "Bearer"
        client.Configuration.set_default(configuration)
        
def load_kubernetes_config() -> None:
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