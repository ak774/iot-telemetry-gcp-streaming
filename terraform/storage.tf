resource "google_storage_bucket" "dataflow" {
  name                        = "iot-gcp-streaming-iot-dataflow"
  location                    = "ASIA-SOUTH1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  lifecycle_rule {
    condition {
      age = 7
    }

    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_iam_member" "dataflow_object_admin" {
  bucket = google_storage_bucket.dataflow.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:iot-dataflow-worker@iot-gcp-streaming.iam.gserviceaccount.com"
}