# GCPDataFlow
Sample GCP Data Flow code to extract data from BigQuery, Transform and write to the Google Storage(Bucket)

Use the below command to run the code in GCP Terminal

python3 \
GCP_DataFlow.py \
--region europe-west1 --output \
gs://<BUCKET_PATH> \
--runner DataflowRunner \
--project <PROJECT_NAME \
--temp_location \
gs:/<BUCKET_PATH>
