terraform {
  required_version = ">= 1.5.0"

  backend "gcs" {
    bucket = "iot-gcp-streaming-tf-state"
    prefix = "terraform/state"
  }

  required_providers {
    google = {
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}