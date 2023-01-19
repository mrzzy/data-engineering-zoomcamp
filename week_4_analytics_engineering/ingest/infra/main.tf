/*
 * Data Engineering Zoomcamp
 * Terraform Deployment
 * NYTaxi INgest Pipeline
*/

locals {
  project_id =  "mrzzy-data-eng-zoomcamp"
  region = "us-west1"
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
  name = "${local.project_id}-nytaxi"
  location = local.region
}

# BigQuery dataset as data warehouse
resource "google_bigquery_dataset" "warehouse" {
  dataset_id = "nytaxi"
  location = local.region
}
