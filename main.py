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
    #스키마 오류 때문에 소스 포맷 csv로 추가해서 csv 생성해서 업로드 시도해봄

    job_config = bigquery.LoadJobConfig(schema=schema, autodetect=False, write_disposition="WRITE_APPEND", source_format=bigquery.SourceFormat.CSV)

    for ticker in tickers:
        ticker_info = fred.get_series_info(ticker)
        ticker_info_df = pd.DataFrame(ticker_info)
        ticker_info_df = ticker_info_df.transpose()

        temp_val = ticker_info_df.columns
        #스키마 오류 때문에 csv로 변경해서 시도
        table_id = f"innate-plexus-345505.fred.fred_meta"
        file = f"./temp/temp.csv"
        ticker_info_df.to_csv(file)

        with open(file, "rb") as source_file:
            print(source_file)
            client.load_table_from_file(source_file, table_id, job_config=job_config)
            table = client.get_table(table_id)
            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), table_id
                )
            )

        #client.load_table_from_dataframe("./temp/temp.csv", 'innate-plexus-345505.fred.fred_meta', job_config=job_config).result()

            print(ticker)

    # ticker_info = fred.get_series_info(ticker)
    # ticker_info_df = pd.DataFrame(ticker_info)
    # meta DB를 생성하고 테이블에서 latest_updated
    sql = f"""
        SELECT  realtime_start
        FROM    innate-plexus-345505.fred.fred_meta
        WHERE   id = 'GDPPOT'

        """
    #bigquery.ScalarQueryParameter('id', 'STRING', ticker)
    query_job = client.query(sql)
    fred_df = query_job.to_dataframe()





