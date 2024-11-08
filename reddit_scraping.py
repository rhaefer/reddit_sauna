import httpx
import pandas as pd
from supabase import create_client, Client
import os
import time
import pandas as pd
from supabase import create_client, Client

# Supabase connection details
SUPABASE_URL = "https://nyngjfovyljrzeqnetgy.supabase.co"  # Replace with your Supabase URL
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im55bmdqZm92eWxqcnplcW5ldGd5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMDgyNzQ4MCwiZXhwIjoyMDQ2NDAzNDgwfQ.3X_jAe8LdkqnBHTyIJyh5Y7_YL5KlxQDhfIup9FKh7c"  # Replace with your Supabase Key

# Initialize Supabase client
def initialize_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# Fetch data from Reddit
def fetch_reddit_data():
    base_url = 'https://www.reddit.com'
    endpoint = '/r/Sauna'
    category = '/new'
    url = base_url + endpoint + category + ".json"

    after_post_id = None
    dataset = []

    for _ in range(5):
        params = {
            'limit': 100,
            't': 'year',
            'after': after_post_id
        }
        response = httpx.get(url, params=params)
        if response.status_code != 200:
            raise Exception('Failed to fetch data')

        json_data = response.json()
        dataset.extend([rec['data'] for rec in json_data['data']['children']])

        after_post_id = json_data['data']['after']
        time.sleep(0.5)
        
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
    

# Supabase credentials
url = "https://nyngjfovyljrzeqnetgy.supabase.co"  # Replace with your Supabase project URL
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im55bmdqZm92eWxqcnplcW5ldGd5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMDgyNzQ4MCwiZXhwIjoyMDQ2NDAzNDgwfQ.3X_jAe8LdkqnBHTyIJyh5Y7_YL5KlxQDhfIup9FKh7c"  # Replace with your Supabase Key
  # Replace with your Supabase API key

# Create a Supabase client
supabase: Client = create_client(url, api_key)

# Initialize variables
batch_size = 1000
start = 0
all_data = []

while True:
    # Fetch data in batches
    response = supabase.table("scraping_table").select("*").range(start, start + batch_size - 1).execute()
    
    # Break the loop if no more data is returned
    if not response.data:
        break

    # Append the fetched data to the list
    all_data.extend(response.data)
    start += batch_size

# Convert all data to a DataFrame
df = pd.DataFrame(all_data)


    


