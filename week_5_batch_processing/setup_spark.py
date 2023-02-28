from logging import fatal
import IPython
from IPython.terminal import embed as ipython
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("homework")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.memory", "6g")
    # Google Cloud Storage
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
    .getOrCreate()
)

hadoop = spark._jsc.hadoopConfiguration() # type: ignore
hadoop.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
hadoop.set('fs.gs.project.id', 'mrzzy-data-eng-zoomcamp')


ipython.embed()
