#
# Data Engineering Zoomcamp
# NYC Taxi Data pipeline
# Shared utilities
#

from datetime import date
from typing import List
import requests
from google.cloud import storage


def load_url_gcs(url: str, bucket: str, path: str) -> str:
    """Download resource & upload it to a GCS bucket.

    Args:
        url:
            Web URL to download the resource from.
        bucket:
            Name of the GCS bucket to load the partition.
        path:
            Path under the GCs bucket to upload the file to.
    Returns:
        GCS URL of of the uploaded partition of data in within the bucket.
    """
    response = requests.get(url)
    blob = storage.Client().bucket(bucket).blob(path)
    blob.upload_from_string(response.content)

    return f"gs://{bucket}/{blob.name}"


def monthly_range(begin: date, end: date) -> List[date]:
    """Create a date range with begin & end with dates on monthly interval.
    Args:
        begin:
            Start of the date range.
        end:
            End (inclusive) of the date range.
    Returns:
        List of dates: start of each month between begin & end.
    """
    # discard day of month by resetting begin & end start of month
    begin, end = date(begin.year, begin.month, 1), date(end.year, end.month, 1)
    # +1: inclusive of end month
    n_months = (end.year - begin.year) * 12 + end.month - begin.month + 1
    if n_months < 1:
        raise ValueError(
            "Expected begin & end to delimit a date range of at least 1 month"
        )
    return [
        date(
            year=begin.year + i // 12,
            month=begin.month + i % 12,
            day=1,
        )
        for i in range(n_months)
    ]
