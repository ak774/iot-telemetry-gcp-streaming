resource "google_pubsub_topic" "telemetry" {
  name = "iot-telemetry"
}

resource "google_pubsub_subscription" "dataflow_telemetry" {
  name  = "dataflow-telemetry-sub"
  topic = google_pubsub_topic.telemetry.id

  ack_deadline_seconds = 10

  message_retention_duration = "604800s"
}

resource "google_pubsub_topic" "telemetry_dlq" {
  name = "iot-telemetry-dlq"
}

resource "google_pubsub_subscription" "telemetry_dlq" {
  name  = "telemetry-dlq-sub"
  topic = google_pubsub_topic.telemetry_dlq.id

  ack_deadline_seconds = 10

  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription_iam_member" "dataflow_subscriber" {
  subscription = google_pubsub_subscription.dataflow_telemetry.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:iot-dataflow-worker@iot-gcp-streaming.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "dataflow_viewer" {
  subscription = google_pubsub_subscription.dataflow_telemetry.name
  role         = "roles/pubsub.viewer"
  member       = "serviceAccount:iot-dataflow-worker@iot-gcp-streaming.iam.gserviceaccount.com"
}

resource "google_pubsub_topic" "telemetry_cd_test" {
  name = "iot-telemetry-cd-test"
}

resource "google_pubsub_subscription" "dataflow_cd_test" {
  name  = "dataflow-cd-test-sub"
  topic = google_pubsub_topic.telemetry_cd_test.id

  ack_deadline_seconds = 10

  message_retention_duration = "604800s"
}

resource "google_pubsub_topic" "telemetry_cd_dlq" {
  name = "iot-telemetry-cd-dlq"
}

resource "google_pubsub_subscription" "telemetry_cd_dlq" {
  name  = "telemetry-cd-dlq-sub"
  topic = google_pubsub_topic.telemetry_cd_dlq.id

  ack_deadline_seconds = 10

  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription_iam_member" "dataflow_cd_test_subscriber" {
  subscription = google_pubsub_subscription.dataflow_cd_test.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${var.dataflow_worker_sa}"
}

resource "google_pubsub_subscription_iam_member" "dataflow_cd_test_viewer" {
  subscription = google_pubsub_subscription.dataflow_cd_test.name
  role         = "roles/pubsub.viewer"
  member       = "serviceAccount:${var.dataflow_worker_sa}"
}

resource "google_pubsub_topic_iam_member" "dataflow_cd_dlq_publisher" {
  topic  = google_pubsub_topic.telemetry_cd_dlq.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${var.dataflow_worker_sa}"
}
