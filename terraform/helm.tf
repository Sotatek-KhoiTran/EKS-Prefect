resource "helm_release" "prefect_server" {
  name             = "prefect-server"
  repository       = "https://prefecthq.github.io/prefect-helm"
  chart            = "prefect-server"
  namespace        = kubernetes_namespace.prefect.metadata[0].name
  create_namespace = false

  values = [
    yamlencode({
      server = {
        uiConfig = {
          prefectUiApiUrl = "http://localhost:4200/api"
        }
      }
      postgresql = {
        primary = {
          persistence = {
            enabled = false
          }
        }
      }
      sqlite = {
        enabled = false
      }
      service = {
        type = "ClusterIP"
      }
    })
  ]

  depends_on = [aws_eks_addon.coredns]
}

resource "helm_release" "spark_operator" {
  name             = "spark-operator"
  repository       = "https://kubeflow.github.io/spark-operator"
  chart            = "spark-operator"
  namespace        = kubernetes_namespace.spark_operator.metadata[0].name
  create_namespace = false

  values = [
    yamlencode({
      controller = {
        serviceAccount = {
          create = false
          name   = kubernetes_service_account.spark_operator_controller.metadata[0].name
        }
        rbac = {
          create = false
        }
        tolerations = [{
          key      = "eks.amazonaws.com/compute-type"
          operator = "Equal"
          value    = "fargate"
          effect   = "NoSchedule"
        }]

        resources = {
          requests = {
            cpu    = "500m"
            memory = "1Gi"
          }

          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }

      spark = {
        serviceAccount = {
          create = false
          name   = kubernetes_service_account.spark_driver.metadata[0].name
        }
        rbac = {
          create = false
        }
        jobNamespaces = ["spark-jobs"]
      }
      webhook = {
        enable = true
        tolerations = [{
          key      = "eks.amazonaws.com/compute-type"
          operator = "Equal"
          value    = "fargate"
          effect   = "NoSchedule"
        }]
      }
      prometheus = {
        metrics = {
          enable = true
        }
      }
    })
  ]

  depends_on = [
    aws_eks_addon.coredns,
    kubernetes_cluster_role_binding.spark_operator_controller,
    kubernetes_role_binding.spark_driver_executor_manager
  ]
}

resource "helm_release" "prefect_worker" {
  name             = "prefect-worker"
  repository       = "https://prefecthq.github.io/prefect-helm"
  chart            = "prefect-worker"
  namespace        = kubernetes_namespace.prefect.metadata[0].name
  create_namespace = false

  values = [
    yamlencode({
      serviceAccount = {
        create = false
        name   = kubernetes_service_account.prefect_worker.metadata[0].name
      }
      worker = {
        config = {
          workPool = var.prefect_work_pool_name
        }
        apiConfig = "selfHostedServer"
        selfHostedServerApiConfig = {
          apiUrl = var.prefect_server_api_url
        }
        clusterUid = var.cluster_name
      }
    })
  ]

  depends_on = [
    helm_release.prefect_server,
    kubernetes_role_binding.prefect_worker_job_manager
  ]
}
