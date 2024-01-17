from google.cloud import bigquery

# To do:
# Connect to BigQuery
# Write Query
# Create Table

# Define project id and table id
PROJECT_ID = "jtp-gcp-data-eng"
TARGET_TABLE_ID = "{}.dwh_bikesharing.dim_regions".format(PROJECT_ID)

def create_dim_table(PROJECT_ID, TARGET_TABLE_ID):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination = TARGET_TABLE_ID,
        write_disposition = "WRITE_TRUNCATE"
    )

    sql = """
        SELECT
            region_id
            , name
        FROM `{}.raw_bikesharing.regions` as regions
        """.format(PROJECT_ID)

    query_job = client.query(sql, job_config = job_config)

    try:
        query_job.result()
        print("Query Success")
    except Exception as exception:
        print(exception)

if __name__ == "__main__":
    create_dim_table(PROJECT_ID, TARGET_TABLE_ID)