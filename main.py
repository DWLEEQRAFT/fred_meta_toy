from fredapi import Fred
import pandas as pd
import glob
from google.cloud import bigquery
from google.oauth2 import service_account



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    key_path = glob.glob("./config/*.json")[0]

    credentials = service_account.Credentials.from_service_account_file(key_path)

    client = bigquery.Client(credentials=credentials,
                             project=credentials.project_id)
    fred = Fred(api_key='51539bf992c74d576a870219fb703109')


    tickers = ['GDPPOT', 'DFEDTAR']
    schema = [ bigquery.SchemaField("id", "STRING"),
               bigquery.SchemaField("realtime_end", "DATE"),
               bigquery.SchemaField("realtime_start", "DATE"),
               bigquery.SchemaField("title", "STRING"),
               bigquery.SchemaField("observation_end", "DATE"),
               bigquery.SchemaField("observation_start", "DATE"),
               bigquery.SchemaField("frequency", "STRING"),
               bigquery.SchemaField("frequency_short", "STRING"),
               bigquery.SchemaField("units", "STRING"),
               bigquery.SchemaField("units_short", "STRING"),
               bigquery.SchemaField("seasonal_adjustment", "STRING"),
               bigquery.SchemaField("seasonal_adjustment_short", "STRING"),
               bigquery.SchemaField("last_updated", "DATETIME"),
               bigquery.SchemaField("popularity", "STRING"),
               bigquery.SchemaField("notes", "STRING"),
              ]
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")

    for ticker in tickers:
        ticker_info = fred.get_series_info(ticker)
        ticker_info_df = pd.DataFrame(ticker_info)
        ticker_info_df = ticker_info_df.transpose()

        client.load_table_from_dataframe(ticker_info_df, 'innate-plexus-345505.fred.fred_meta', job_config=job_config).result()
        print(ticker)

    # ticker_info = fred.get_series_info(ticker)
    # ticker_info_df = pd.DataFrame(ticker_info)
    # meta DB를 생성하고 테이블에서 latest_updated
    sql = f"""
        SELECT  realtime_start
        FROM    innate-plexus-345505.fred.fred_meta
        WHERE   id = @id

        """
    bigquery.ScalarQueryParameter('id', 'STRING', ticker)
    query_job = client.query(sql)
    fred_df = query_job.to_dataframe()





