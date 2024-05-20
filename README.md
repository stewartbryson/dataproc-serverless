# Samples for Dataproc Serverless
First, push the code to a bucket:

```
gsutil samples/gcs_bigquery.py gs://<bucket-name>
```

Then execute the Dataproc Batch using gcloud:
```
gcloud dataproc batches submit pyspark \
gs://<bucket-name>/gcs_bigquery.py \
 --project <project-name> \
 --deps-bucket <bucket-name> --region us-central1 \
 --jars gs://<bucket-name>/spark-bigquery-with-dependencies_2.13-0.36.2.jar \
 --version 2.0
 ```