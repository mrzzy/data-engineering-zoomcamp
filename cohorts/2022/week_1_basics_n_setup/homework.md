## Week 1 Homework

In this homework we'll prepare the environment 
and practice with terraform and SQL


## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have? 

```
Google Cloud SDK 382.0.0
alpha 2022.04.15
beta 2022.04.15
bq 2.0.74
bundled-python3-unix 3.8.11
core 2022.04.15
gsutil 5.9
minikube 1.25.2
skaffold 1.38.0
```

To get the version, run `gcloud --version`

## Google Cloud account 

Create an account in Google Cloud and create a project.


## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
```
Initializing the backend...

Successfully configured the backend "local"! Terraform will automatically
use this backend unless the backend configuration changes.

Initializing provider plugins...
- Finding latest version of hashicorp/google...
- Installing hashicorp/google v4.26.0...
- Installed hashicorp/google v4.26.0 (signed by HashiCorp)

Terraform has created a lock file .terraform.lock.hcl to record the provider
selections it made above. Include this file in your version control repository
so that Terraform can guarantee to make the same selections by default when
you run "terraform init" in the future.

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.
```
* `terraform plan`
```


Terraform used the selected providers to generate the following execution
plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "mrzzy-data-eng-zoomcamp"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + dataset {
              + target_types = (known after apply)

              + dataset {
                  + dataset_id = (known after apply)
                  + project_id = (known after apply)
                }
            }

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "dtc_data_lake_mrzzy-data-eng-zoomcamp"
      + project                     = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_storage_class = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

─────────────────────────────────────────────────────────────────────────────

Note: You didn't use the -out option to save this plan, so Terraform can't
guarantee to take exactly these actions if you run "terraform apply" now.
```


* `terraform apply` 
```
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "mrzzy-data-eng-zoomcamp"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + dataset {
              + target_types = (known after apply)

              + dataset {
                  + dataset_id = (known after apply)
                  + project_id = (known after apply)
                }
            }

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "dtc_data_lake_mrzzy-data-eng-zoomcamp"
      + project                     = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_storage_class = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.
google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_bigquery_dataset.dataset: Creation complete after 2s [id=projects/mrzzy-data-eng-zoomcamp/datasets/trips_data_all]
zgoogle_storage_bucket.data-lake-bucket: Creation complete after 4s [id=dtc_data_lake_mrzzy-data-eng-zoomcamp]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```

Apply the plan and copy the output (after running `apply`) to the form.

It should be the entire output - from the moment you typed `terraform init` to the very end.

## Prepare Postgres 

Run Postgres and load data as shown in the videos

We'll use the yellow taxi trips from January 2021:

```bash
wget https://s3.amazonaws.com/nyc-tlc/csv_backup/yellow_tripdata_2021-01.csv
```

You will also need the dataset with zones:

```bash 
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

Download this data and put it to Postgres

## Question 3. Count records 

How many taxi trips were there on January 15? `53024`

```sql
SELECT COUNT(*) AS "No. Trips" FROM "TaxiTrips"
WHERE tpep_pickup_datetime BETWEEN '2021-01-15 00:00:00' AND '2021-01-15 23:59:59';
````

Consider only trips that started on January 15.


## Question 4. Largest tip for each day

Find the largest tip for each day. 
```sql
SELECT day, MAX(tip_amount) AS "Largest Tip"
FROM (
    SELECT CAST(tpep_pickup_datetime AS date) AS day, tip_amount
    FROM "TaxiTrips"
)
GROUP BY day;
```
+------------+---------------+
| day        | Largest Tip   |
|------------+---------------|
| 2021-01-21 | 166.0         |
| 2020-12-31 | 4.08          |
| 2021-01-10 | 91.0          |
| 2021-01-05 | 151.0         |
| 2021-01-07 | 95.0          |
| 2021-01-24 | 122.0         |
| 2009-01-01 | 0.0           |
| 2021-01-09 | 230.0         |
| 2021-01-17 | 65.0          |
| 2021-01-11 | 145.0         |
| 2021-01-29 | 75.0          |
| 2021-01-16 | 100.0         |
| 2021-01-15 | 99.0          |
| 2021-01-14 | 95.0          |
| 2021-02-22 | 1.76          |
| 2021-01-27 | 100.0         |
| 2021-01-30 | 199.12        |
| 2021-02-01 | 1.54          |
| 2021-01-22 | 92.55         |
| 2021-01-25 | 100.16        |
| 2021-01-23 | 100.0         |
| 2021-01-13 | 100.0         |
| 2021-01-01 | 158.0         |
| 2021-01-08 | 100.0         |
| 2021-01-12 | 192.61        |
| 2021-01-31 | 108.5         |
| 2021-01-06 | 100.0         |
| 2021-01-04 | 696.48        |
| 2021-01-02 | 109.15        |
| 2021-01-28 | 77.14         |
| 2021-01-26 | 250.0         |
| 2008-12-31 | 0.0           |
| 2021-01-20 | 1140.44       |
| 2021-01-03 | 369.4         |
| 2021-01-18 | 90.0          |
| 2021-01-19 | 200.8         |
+------------+---------------+

On which day it was the largest tip in January? `2021-01-20`

```sql
SELECT CAST(tpep_pickup_datetime AS date) AS day
FROM "TaxiTrips"
WHERE tip_amount = (
    SELECT MAX(tip_amount)
    FROM "TaxiTrips"
    WHERE tpep_pickup_datetime 
        BETWEEN '2021-01-01 00:00:00' AND '2021-01-31 23:59:59'
);
```


Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")


## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14? `Upper East Side North`

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown" 

```sql
WITH destination_counts AS (
    SELECT COALESCE(dropoff."Zone", 'Unknown') AS destination, COUNT(*) AS occurances
    FROM "TaxiTrips" trip
        LEFT JOIN "PickupZones" pickup ON pickup."LocationID" = trip."PULocationID"
        LEFT JOIN "PickupZones" dropoff ON dropoff."LocationID" = trip."DOLocationID"
    WHERE pickup."Zone" = 'Central Park'
    GROUP BY dropoff."Zone"
)
SELECT destination
FROM destination_counts
WHERE occurances = (SELECT MAX(occurances) FROM destination_counts);
```

## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)? `Alphabet City / Unknown`

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"


If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East". 

```sql
WITH pair_avg_price AS (
    SELECT
        COALESCE(pickup."Zone", 'Unknown') || ' / ' || COALESCE(dropoff."Zone", 'Unknown') AS pair,
        AVG(trip.total_amount) AS avg_price
    FROM "TaxiTrips" trip
        LEFT JOIN "PickupZones" pickup ON pickup."LocationID" = trip."PULocationID"
        LEFT JOIN "PickupZones" dropoff ON dropoff."LocationID" = trip."DOLocationID"
    GROUP BY pickup."Zone", dropoff."Zone"
)
SELECT pair FROM pair_avg_price
WHERE avg_price = (SELECT MAX(avg_price) FROM pair_avg_price);
```


## Submitting the solutions

* Form for submitting: https://forms.gle/yGQrkgRdVbiFs8Vd7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Wednesday), 22:00 CET


## Solution

Here is the solution to questions 3-6: [video](https://www.youtube.com/watch?v=HxHqH2ARfxM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

