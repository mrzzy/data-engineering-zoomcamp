/*
 * Data Engineering Zoomcamp
 * Terraform Deployment
 * NYTaxi Ingest Pipeline
*/

locals {
  project_id = "mrzzy-data-eng-zoomcamp"
  region     = "us-west1"
}

terraform {
  cloud {
    organization = "mrzzy-co"

    workspaces {
      name = "data-engineering-zoomcamp"
    }
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.49.0"
    }
  }
}

provider "google" {
  project = local.project_id
  # region: US, West, Oregon
  region = local.region
  zone   = "${local.region}-c"
}

# GCS bucket as data lake
resource "google_storage_bucket" "lake" {
  name     = "${local.project_id}-nytaxi"
  location = local.region
}

# GCS bucket for prefect storage
resource "google_storage_bucket" "prefect" {
  name     = "${local.project_id}-prefect"
  location = local.region
}

# BigQuery dataset as data warehouse
resource "google_bigquery_dataset" "warehouse" {
  dataset_id = "nytaxi"
  location   = local.region
}
# GKE cluster to provide compute for data processing
resource "google_container_cluster" "compute" {
  name             = "data-proc-compute"
  description      = "Cluster providing compute for data processing"
  location         = local.region
  enable_autopilot = true
  # needed as autopiliot clusters are vpc VPC-native by default
  ip_allocation_policy {}

  node_config {
    spot            = true
    disk_size_gb    = 20
    service_account = google_service_account.pipeline.email
  }
}

# IAM: Ingest Pipeline Service Account
resource "google_service_account" "pipeline" {
  account_id  = "pipeline"
  description = "Service Account to authenticate Ingest pipeline"
}

# IAM role bindings
resource "google_project_iam_binding" "gcs" {
  project = local.project_id
  role    = "roles/storage.objectAdmin"
  members = [
    google_service_account.pipeline.member
  ]
}

resource "google_project_iam_binding" "warehouse" {
  for_each = toset(["roles/bigquery.jobUser", "roles/bigquery.dataEditor"])
  project  = local.project_id
  role     = each.key
  members = [
    google_service_account.pipeline.member
  ]
}
