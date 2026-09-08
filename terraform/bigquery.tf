resource "google_bigquery_table" "pipeline_deployments" {
  project    = var.project_id
  dataset_id = "iot_silver"
  table_id   = "pipeline_deployments"

  time_partitioning {
    type  = "DAY"
    field = "deployed_at"
  }

  clustering = ["application", "environment", "status"]

  schema = <<EOF
[
  {
    "name": "application",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "environment",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "deployment_id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "job_name",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "job_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "commit_sha",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "deployed_at",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "previous_job_name",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF
}