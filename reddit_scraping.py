import httpx
import pandas as pd
import os
import time
from supabase import create_client, Client

# Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
def initialize_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# Fetch data from Reddit with enhanced debugging and User-Agent header
def fetch_reddit_data():
    base_url = 'https://www.reddit.com'
    endpoint = '/r/Sauna'
    category = '/new'
    url = base_url + endpoint + category + ".json"

    after_post_id = None
    dataset = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for _ in range(5):
        params = {
            'limit': 100,
            't': 'year',
            'after': after_post_id
        }
        try:
            response = httpx.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()  # Check if the request was successful
            json_data = response.json()
            dataset.extend([rec['data'] for rec in json_data['data']['children']])

            after_post_id = json_data['data']['after']
            if not after_post_id:  # Exit if there's no more data
                break

            time.sleep(0.5)  # Rate-limiting to prevent bans
        except httpx.RequestError as e:
            print(f"Request error: {e}")
            raise Exception("Failed to fetch data due to a network issue.")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e}")
            raise Exception("Failed to fetch data due to a bad status code.")

    return pd.DataFrame(dataset)

# Append data to Supabase
def append_to_supabase(df, supabase_client):
    data_records = df.to_dict(orient='records')  # Convert DataFrame to list of dictionaries
    
    for record in data_records:
        response = supabase_client.table("scraping_table").insert(record).execute()
        
        # Check if 'data' exists in the response to verify insertion
        if response.data:  # If data is present, insertion succeeded
            print("Record inserted successfully.")
        else:
            print(f"Error inserting record: {response}")

if __name__ == "__main__":
    # Fetch data and save to Supabase
    supabase_client = initialize_supabase()
    df = fetch_reddit_data()
    df = df[['id','subreddit', 'title','upvote_ratio','link_flair_text','score','author','num_comments','permalink','url','selftext']]
    append_to_supabase(df, supabase_client)

    


