from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime

args = {
	'owner' : 'airflow', 
	'start_date' : datetime(2023, 4, 23), 
	'email' : ['asralfa9@gmail.com'], 	
	'email_on_failure' : False, 
	'email_on_retry' : False,  
	'retries' : 1,  
	'retry_delay' : timedelta(minutes = 1)
}									

dag = DAG(
	'REDDIT_ETL_DAG_v5',  
	default_args = args,  
	description = 'Reddit extraction'
	)
																
task_1 = BashOperator(
    task_id='install_pandas_libraries',
    bash_command='pip install pandas',
    dag=dag,
)

task_2 = BashOperator(
    task_id='install_boto3_libraries',
    bash_command='pip install boto3 s3fs',
    dag=dag,
)


def reddit_extract():
    import pandas as pd 
    import boto3
    import s3fs
    import json
    from datetime import datetime
    import requests
    headers = {
    'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    }
    
    urls = [
        'https://www.reddit.com/r/elon/comments.json?limit=100', 
        'https://www.reddit.com/r/elonmusk/comments.json?limit=100'
    ]
    
    df_list = list()

    for url in urls:
        response = requests.get(url, headers = headers).json()

        sub_reddits = [sub['data']['link_title'] for sub in response['data']['children']]
        sub_ids = [sub['data']['subreddit_id'] for sub in response['data']['children']]
        sub_utc = [datetime.utcfromtimestamp(sub['data']['created_utc']) for sub in response['data']['children']]
        user_ids = [sub['data']['link_author'] for sub in response['data']['children']]
        sub_ups = [sub['data']['ups'] for sub in response['data']['children']]

        sub_info = list(zip(sub_ids, user_ids, sub_reddits, sub_ups, sub_utc))

        sub_df = pd.DataFrame(sub_info, columns = ['sub_ids', 'user_ids', 'sub_reddits', 'sub_ups', 'sub_utc'])

        df_list.append(sub_df)

    df = pd.concat(df_list)
    df.reset_index(inplace = True, drop = True)

    # dataframe to csv
    local_file_name = 'ElonMusk_Reddit_Data.csv'
    df.to_csv(csv_filename, index = False)

    ACCESS_KEY = 'AKIARJLANTBP7HLFB3OJ'
    SECRET_KEY = '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'
    
    session = boto3.Session(
    aws_access_key_id=ACCESS_KEY, 
    aws_secret_access_key=SECRET_KEY
    )
    
    s3 = session.resource('s3')

    # Selecting A Bucket
    BUCKET_NAME = 'reddit-data-pipeline-airflow'
    bucket = s3.Bucket(BUCKET_NAME)

    # Uploading A File To Your S3 Bucket
    bucket.upload_file('ElonMusk_Reddit_Data.csv')


task_3 = PythonOperator(	
	task_id = 'ETLreddit', 
	dag = dag, 
	python_callable = reddit_extract
	)
						
task_1 >> task_2 >> task_3
