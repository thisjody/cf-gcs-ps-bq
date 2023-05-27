gcloud functions deploy cf-gcs-ps-bq \
  --gen2 \
  --runtime=python311 \
  --region=us-west1 \
  --source=src \
  --entry-point=bq_load_from_gcs \
  --memory 16384MB \
  --timeout 540s  \
  --trigger-topic ps-df-bigquery-ingest