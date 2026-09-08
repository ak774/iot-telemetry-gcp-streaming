variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "iot-gcp-streaming"
}

variable "region" {
  description = "Primary GCP region"
  type        = string
  default     = "asia-south1"
}

variable "dataflow_worker_sa" {
  description = "Dedicated Dataflow worker service account"
  type        = string
  default     = "iot-dataflow-worker@iot-gcp-streaming.iam.gserviceaccount.com"
}