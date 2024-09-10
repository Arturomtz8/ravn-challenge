from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def write_bq(
    bq_table,
    df,
):
    google_project_id = Secret.load("google-project-id")
    gcp_credentials = GcpCredentials.load("gcp-credentials-zoomcamp")

    try:
        df.to_gbq(
            destination_table=bq_table,
            project_id=google_project_id.get(),
            credentials=gcp_credentials.get_credentials_from_service_account(),
            if_exists="replace",
        )
    except Exception as e:
        print(f"Exception encountered {e}")


@flow()
def update_bq_table(bq_table, df):
    write_bq(bq_table, df)
