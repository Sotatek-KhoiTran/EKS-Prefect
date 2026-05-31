resource "kubernetes_namespace" "prefect" {
  metadata {
    name = "prefect"
  }

  timeouts {
    delete = "20m"
  }

  depends_on = [aws_eks_fargate_profile.prefect]
}

resource "kubernetes_namespace" "spark_operator" {
  metadata {
    name = "spark-operator"
  }

  timeouts {
    delete = "20m"
  }

  depends_on = [aws_eks_fargate_profile.spark_operator]
}

resource "kubernetes_namespace" "spark_jobs" {
  metadata {
    name = "spark-jobs"
  }

  timeouts {
    delete = "20m"
  }

  depends_on = [aws_eks_fargate_profile.spark_jobs]
}

resource "kubernetes_service_account" "prefect_worker" {
  metadata {
    name      = "prefect-worker"
    namespace = kubernetes_namespace.prefect.metadata[0].name
  }
}

resource "kubernetes_service_account" "prefect_flow_run" {
  metadata {
    name      = "prefect-flow-run"
    namespace = kubernetes_namespace.prefect.metadata[0].name
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.prefect_flow_run.arn
    }
  }
}

resource "kubernetes_service_account" "spark_operator_controller" {
  metadata {
    name      = "spark-operator-controller"
    namespace = kubernetes_namespace.spark_operator.metadata[0].name
  }
}

resource "kubernetes_service_account" "spark_driver" {
  metadata {
    name      = "spark-driver-sa"
    namespace = kubernetes_namespace.spark_jobs.metadata[0].name
    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.spark_driver.arn
    }
  }
}

resource "kubernetes_role" "prefect_worker_job_manager" {
  metadata {
    name      = "prefect-worker-job-manager"
    namespace = kubernetes_namespace.prefect.metadata[0].name
  }

  rule {
    api_groups = ["batch"]
    resources  = ["jobs"]
    verbs      = ["create", "get", "list", "watch", "delete", "patch"]
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "events", "configmaps", "secrets"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding" "prefect_worker_job_manager" {
  metadata {
    name      = "prefect-worker-job-manager"
    namespace = kubernetes_namespace.prefect.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.prefect_worker.metadata[0].name
    namespace = kubernetes_namespace.prefect.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.prefect_worker_job_manager.metadata[0].name
  }
}

resource "kubernetes_role" "prefect_flow_run_reader" {
  metadata {
    name      = "prefect-flow-run-reader"
    namespace = kubernetes_namespace.prefect.metadata[0].name
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "events"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding" "prefect_flow_run_reader" {
  metadata {
    name      = "prefect-flow-run-reader"
    namespace = kubernetes_namespace.prefect.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.prefect_flow_run.metadata[0].name
    namespace = kubernetes_namespace.prefect.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.prefect_flow_run_reader.metadata[0].name
  }
}

resource "kubernetes_role" "prefect_flow_run_sparkapplication_manager" {
  metadata {
    name      = "prefect-flow-run-sparkapplication-manager"
    namespace = kubernetes_namespace.spark_jobs.metadata[0].name
  }

  rule {
    api_groups = ["sparkoperator.k8s.io"]
    resources  = ["sparkapplications", "sparkapplications/status"]
    verbs      = ["create", "get", "list", "watch", "update", "patch", "delete"]
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "events", "services", "configmaps"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding" "prefect_flow_run_sparkapplication_manager" {
  metadata {
    name      = "prefect-flow-run-sparkapplication-manager"
    namespace = kubernetes_namespace.spark_jobs.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.prefect_flow_run.metadata[0].name
    namespace = kubernetes_namespace.prefect.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.prefect_flow_run_sparkapplication_manager.metadata[0].name
  }
}

resource "kubernetes_cluster_role" "spark_operator_controller" {
  metadata {
    name = "spark-operator-controller"
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "services", "configmaps", "persistentvolumeclaims", "events"]
    verbs      = ["create", "get", "list", "watch", "update", "patch", "delete"]
  }

  rule {
    api_groups = [""]
    resources  = ["namespaces", "nodes"]
    verbs      = ["get", "list", "watch"]
  }

  rule {
    api_groups = ["apps"]
    resources  = ["deployments"]
    verbs      = ["get", "list", "watch"]
  }

  rule {
    api_groups = ["coordination.k8s.io"]
    resources  = ["leases"]
    verbs      = ["create", "get", "list", "watch", "update", "patch", "delete"]
  }

  rule {
    api_groups = ["sparkoperator.k8s.io"]
    resources = [
      "sparkapplications",
      "sparkapplications/status",
      "sparkapplications/finalizers",
      "scheduledsparkapplications",
      "scheduledsparkapplications/status",
      "scheduledsparkapplications/finalizers",
      "sparkconnects",
      "sparkconnects/status",
      "sparkconnects/finalizers"
    ]
    verbs = ["create", "get", "list", "watch", "update", "patch", "delete"]
  }
}

resource "kubernetes_cluster_role_binding" "spark_operator_controller" {
  metadata {
    name = "spark-operator-controller"
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.spark_operator_controller.metadata[0].name
    namespace = kubernetes_namespace.spark_operator.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.spark_operator_controller.metadata[0].name
  }
}

resource "kubernetes_role" "spark_driver_executor_manager" {
  metadata {
    name      = "spark-driver-executor-manager"
    namespace = kubernetes_namespace.spark_jobs.metadata[0].name
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "services", "configmaps", "persistentvolumeclaims"]
    verbs      = ["create", "get", "list", "watch", "delete", "patch", "deletecollection"]
  }

  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["create", "get", "list", "watch", "patch"]
  }
}

resource "kubernetes_role_binding" "spark_driver_executor_manager" {
  metadata {
    name      = "spark-driver-executor-manager"
    namespace = kubernetes_namespace.spark_jobs.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.spark_driver.metadata[0].name
    namespace = kubernetes_namespace.spark_jobs.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.spark_driver_executor_manager.metadata[0].name
  }
}
