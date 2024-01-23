from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
from datetime import datetime

# Replace with your specific values
pipeline_name = "adls_excel_ingestion"
resource_group_name = "mnus-rg-dev-cptgbatch-001"
factory_name = "managed-airflow-001"

# Define the DAG configuration
with DAG(
    dag_id="dag_from_repo_22",
    start_date=datetime(2024, 1, 17),  # Set your desired start date
    schedule_interval=None,  # Adjust as needed
    catchup=False,
) as dag:

    # Create the AzureDataFactoryHook
    adf_hook = AzureDataFactoryHook(
        azure_data_factory_conn_id="azure_default"
    )

    # Define a Python function to trigger the pipeline run
    def trigger_pipeline():
        adf_hook.run_pipeline(
            pipeline_name=pipeline_name,
            resource_group_name=resource_group_name,
            factory_name=factory_name,
        )

    # Create a PythonOperator to execute the pipeline trigger function
    trigger_pipeline_task = PythonOperator(
        task_id="trigger_pipeline",
        python_callable=trigger_pipeline,
    )

    # Define the DAG's task execution flow
    trigger_pipeline_task
