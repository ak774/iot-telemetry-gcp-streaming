resource "google_bigquery_dataset_iam_member" "raw_data_editor" {
  project    = var.project_id
  dataset_id = "iot_raw"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.dataflow_worker_sa}"
}

resource "google_bigquery_dataset_iam_member" "silver_data_editor" {
  project    = var.project_id
  dataset_id = "iot_silver"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.dataflow_worker_sa}"
}

resource "google_bigquery_dataset_iam_member" "gold_data_editor" {
  project    = var.project_id
  dataset_id = "iot_gold"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.dataflow_worker_sa}"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${var.dataflow_worker_sa}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.dataflow_worker_sa}"
}

resource "google_project_iam_member" "monitoring_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${var.dataflow_worker_sa}"
}

resource "google_service_account_iam_member" "dataflow_deployer" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.dataflow_worker_sa}"
  role               = "roles/iam.serviceAccountUser"
  member             = "user:axu04103@gmail.com"
}

resource "google_service_account_iam_member" "cicd_deployer" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.dataflow_worker_sa}"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:iot-cicd-deployer@iot-gcp-streaming.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "monitoring_alert_policy_editor" {
  project = var.project_id
  role    = "roles/monitoring.alertPolicyEditor"
  member  = "serviceAccount:iot-cicd-deployer@iot-gcp-streaming.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "monitoring_dashboard_editor" {
  project = var.project_id
  role    = "roles/monitoring.dashboardEditor"
  member  = "serviceAccount:iot-cicd-deployer@iot-gcp-streaming.iam.gserviceaccount.com"
}