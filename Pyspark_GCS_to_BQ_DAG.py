import airflow
from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from datetime import timedelta,date
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator,DataprocSubmitJobOperator,DataprocDeleteClusterOperator

# config
PROJECT_ID="project-b5a1bfde-12d9-4a81-8bc"
REGION="us-east1"
CLUSTER_NAME="demo-cluster"
ARGS = {
    'owner':'Sudhir Kulkarni',
    'start_date': days_ago(1),
    'deped_on_past': False,
    'email_on_failure':False,
    'email_on_retry':False,
    'email_on_success':False,
    'email':['sudhirkulkarni65@gmail.com'],
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

CLUSTER_CONFIG = {
    "cluster_type": "STANDARD",
    "cluster_tier": "CLUSTER_TIER_STANDARD",
    "engine": "DEFAULT",
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 32},
    },
   
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": "gs://pyspark-practice-1/Pyspark_GCS_to_BQ.py"},
}


# Define the DAG
with DAG(
    dag_id="Pyspark_to_BQ",
    schedule_interval="0 5 * * *",         # everyday at 5 AM
    description="DAG to move data from GCS to BQ using pyspark",\
    default_args=ARGS, 
    tags=["Rocket_Team","Pyspark","dataproc","GCS","Data_Pipeline"],                    
) as dag:

        

    # Define a task
        create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
        submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )
        delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )
            

    

    # If you had multiple tasks, you could set dependencies like:
create_cluster >>  submit_job >> delete_cluster
