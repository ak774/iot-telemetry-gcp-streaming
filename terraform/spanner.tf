resource "google_spanner_instance" "iot_streaming" {
  name             = "iot-streaming"
  config           = "regional-asia-south1"
  display_name     = "IoT streaming durable state"
  processing_units = 100
  edition          = "STANDARD"
}

resource "google_spanner_database" "iot_registry" {
  instance         = google_spanner_instance.iot_streaming.name
  name             = "iot_registry"
  database_dialect = "GOOGLE_STANDARD_SQL"
}

resource "google_spanner_database_iam_member" "dataflow_database_user" {
  instance = google_spanner_instance.iot_streaming.name
  database = google_spanner_database.iot_registry.name
  role     = "roles/spanner.databaseUser"
  member   = "serviceAccount:iot-dataflow-worker@iot-gcp-streaming.iam.gserviceaccount.com"
}