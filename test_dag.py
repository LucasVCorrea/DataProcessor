from airflow.sdk import dag, task

@dag("aset_test_dag")
def aset_test_dag():

    @task(task_id="fetch_data")
    def fetch_data():
        import gspread
        import pandas as pd
        from oauth2client.service_account import ServiceAccountCredentials

        # Define scope and authenticate
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('/data/credentials.json', scope)
        client = gspread.authorize(creds)

        # Open the sheet by name or ID
        sh = client.open("test")
        worksheet = sh.sheet1

        data = worksheet.get_all_records()
        pd.DataFrame(data).to_parquet("/data/fetched/fetched_data.parquet")

    @task(task_id="summarize_data")
    def summarize_data():
        import pandas as pd

        df = pd.read_parquet("/data/fetched/fetched_data.parquet")
        df = df[["cats","dogs","people"]].sum().to_frame().T
        print("sumarized\n",df)
        df.to_parquet("/data/sumarized/sumarized_data.parquet")

    @task(task_id="aggregate_data")
    def aggregate_data():
        import pandas as pd

        df = pd.read_parquet("/data/fetched/fetched_data.parquet")
        df["entities"] = df.cats + df.dogs + df.people
        print("mean\n",df)
        df.to_parquet("/data/aggregated/mean_data.parquet")

    fetch_data = fetch_data()
    summarize_data = summarize_data()
    aggregate_data = aggregate_data()

    fetch_data >> [summarize_data, aggregate_data]

dag = aset_test_dag()