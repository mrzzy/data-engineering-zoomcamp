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
  location = "US"
}

# BigQuery dataset as data warehouse
resource "google_bigquery_dataset" "warehouse_dev" {
  dataset_id = "nytaxi"
  location   = local.region
}

resource "google_bigquery_dataset" "warehouse_prod" {
  dataset_id = "nytaxi_prod"
  location   = local.region
}

# GKE cluster to provide compute for data processing
resource "google_container_cluster" "compute" {
  name             = "data-proc-compute"
  description      = "Cluster providing compute for data processing"
  location         = local.region
  enable_autopilot = true
  # needed as autopliot clusters are vpc VPC-native by default
  ip_allocation_policy {}
}

# Artifact Repository to store built containers
resource "google_artifact_registry_repository" "docker" {
  location      = local.region
  repository_id = "docker"
  format        = "DOCKER"
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

# allow 'pipeline' k8s service account to impersonate pipeline service account
resource "google_service_account_iam_binding" "k8s_sa" {
  service_account_id = google_service_account.pipeline.name
  role               = "roles/iam.workloadIdentityUser"
  members = [
    "serviceAccount:${local.project_id}.svc.id.goog[default/pipeline]"
  ]
}
