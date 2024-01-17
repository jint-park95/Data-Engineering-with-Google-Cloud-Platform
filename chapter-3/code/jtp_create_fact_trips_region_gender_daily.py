import sys
from google.cloud import bigquery

# Project ID and target table variables
PROJECT_ID = "jtp-gcp-data-eng"
TARGET_TABLE_ID = "{}.dwh_bikesharing.fact_region_gender_daily".format(PROJECT_ID)

def create_fact_table(PROJECT_ID, TARGET_TABLE_ID):
    # load_date = "2018-01-02"
    load_date = sys.argv[1]
    print("\nLoad date:", load_date)

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination = TARGET_TABLE_ID,
        write_disposition = "WRITE_APPEND"
    )

    sql = """
        SELECT
            DATE(start_date) as trip_date,
            CAST(region_id AS INTEGER) as region_id,
            member_gender,
            count(trip_id) as total_trips
        FROM `{PROJECT_ID}.raw_bikesharing.trips` as trips
        JOIN `{PROJECT_ID}.raw_bikesharing.stations` as stations
            ON trips.start_station_id = stations.station_id
        WHERE DATE(start_date) = DATE('{LOAD_DATE}') AND member_gender IS NOT NULL
        GROUP BY
            trip_date,
            region_id,
            member_gender
        """.format(PROJECT_ID = PROJECT_ID, LOAD_DATE=load_date)

    query_job = client.query(sql, job_config=job_config)

    # print(PROJECT_ID, TARGET_TABLE_ID)
    # print(sql)

    try:
        query_job.result()
        print("Query success")
    except Exception as exception:
        print(exception)

if __name__ == '__main__':
    create_fact_table(PROJECT_ID, TARGET_TABLE_ID)