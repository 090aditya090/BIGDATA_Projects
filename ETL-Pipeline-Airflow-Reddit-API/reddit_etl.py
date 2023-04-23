pip install s3fs
pip install requests
# imports the necessary libraries: 
#  pandas for data manipulation
#  s3fs for working with Amazon S3
#  json for JSON parsing 
#  datetime for datetime manipulation
#  requests for making HTTP requests.
import pandas as pd 
import s3fs
import json
from datetime import datetime
import requests

# User-Agent key with a string value that specifies the user agent for the HTTP request. 
# This helps identify the client making the request to the server.
def reddit_extract():
    headers = {
    'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'

    }
    

# urls that contains two URLs, each pointing to the JSON data for a different subreddit: r/elon and r/elonmusk.
    urls = [
        'https://www.reddit.com/r/elon/comments.json?limit=100', 
        'https://www.reddit.com/r/elonmusk/comments.json?limit=100'
    ]
    
    df_list = list()

    # This code initiates a for loop that iterates through each URL in the urls list. 
    # For each URL, an HTTP GET request is sent using the requests library, with the specified headers. 
    # The response is stored in the response variable as a JSON object.
    for url in urls:
        response = requests.get(url, headers = headers).json()

        sub_reddits = [sub['data']['link_title'] for sub in response['data']['children']]
        sub_ids = [sub['data']['subreddit_id'] for sub in response['data']['children']]
        sub_utc = [datetime.utcfromtimestamp(sub['data']['created_utc']) for sub in response['data']['children']]
        user_ids = [sub['data']['link_author'] for sub in response['data']['children']]
        sub_ups = [sub['data']['ups'] for sub in response['data']['children']]


        # This code combines the extracted data for each post into a list of tuples using the built-in zip() function.
        sub_info = list(zip(sub_ids, user_ids, sub_reddits, sub_ups, sub_utc))

        # This code creates a new Pandas DataFrame from the list of tuples. The column names for the DataFrame are specified using the columns parameter.
        sub_df = pd.DataFrame(sub_info, columns = ['sub_ids', 'user_ids', 'sub_reddits', 'sub_ups', 'sub_utc'])

        # This code appends the newly created DataFrame to the df_list list.
        df_list.append(sub_df)

    # This code concatenates all of the DataFrames
    df = pd.concat(df_list)
    df.reset_index(inplace = True, drop = True)

    df.to_csv('s3://reddit-data-pipeline-airflow/reddit-data/reddit-data.csv', index = False)

