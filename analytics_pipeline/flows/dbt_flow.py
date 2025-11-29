from prefect import flow, task
import subprocess
import os
# from dotenv import load_dotenv

#load_dotenv()

@task(name="Run dbt job")
def run_dbt_job(command: str):
    dbt_project_path = os.path.join(os.getcwd(), "analytics_pipeline")
    
    full_command = [
        "dbt", command, "--select", "marts+", "--project-dir", dbt_project_path
    ]

    print(f"Executing: {' '.join(full_command)}")

    subprocess.run(full_command, check=True)


@flow(name="E-commerce Metrics Pipeline")
def dbt_pipeline_flow():
    
    run_dbt_job(command="run")
    run_dbt_job(command="test")

if __name__ == "__main__":
    dbt_pipeline_flow.serve(
        name="e-commerce-dbt-deployment",
        tags=["local"],
        # cron="0 2 * * *", # Every day at 2 AM
    )