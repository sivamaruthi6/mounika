from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from datetime import datetime

PROJECT = "vinayaka-476014"
REGION = "us-central1"
BUCKET = "proj_002"  # bucket name only

with DAG(
    dag_id="beam_avro_pipeline_flex",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
):

    run_beam_job = DataflowStartFlexTemplateOperator(
        task_id="run_beam_job",
        body={
            "launchParameter": {
                "jobName": "beam-avro-job",
                "containerSpecGcsPath": f"gs://{BUCKET}/templates/beam_job.json",
                "parameters": {
                    "input": f"gs://{BUCKET}/sno100.csv",
                    "output": f"gs://{BUCKET}/output/employee",
                    "schema": f"gs://{BUCKET}/schema.avsc",
                    "runner": "DataflowRunner",
                    "temp_location": f"gs://{BUCKET}/temp",
                    "staging_location": f"gs://{BUCKET}/staging",
                },
            }
        },
        project_id=PROJECT,
        location=REGION,
    )



