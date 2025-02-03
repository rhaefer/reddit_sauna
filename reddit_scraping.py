import praw
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

import mimetypes

def get_image_url(post):
    """Extract the correct image URL from a Reddit post."""
    # If the post contains media metadata (gallery)
    if hasattr(post, "media_metadata") and post.media_metadata:
        for item in post.media_metadata.values():
            if "s" in item and "u" in item["s"]:  # Check for available media
                return item["s"]["u"].replace("&amp;", "&")  # Correct encoding
    
    # If the post's URL is an image (jpg, png, gif)
    mime_type, _ = mimetypes.guess_type(post.url)
    if mime_type and mime_type.startswith("image"):
        return post.url
    
    # If no image found, return None
    return None

# Initialize Supabase client
def initialize_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize PRAW with your credentials
reddit = praw.Reddit(
    client_id="hTujd0OmhH9KsGFt_jut4Q",
    client_secret="WWCqKzEDpktPGdT9-CZ8sAaV28bj2A",
    user_agent="sauna app v1.0 by /u/reidhaefer"  # e.g., 'myAppName v1.0 by /u/your_reddit_username'
)

 #Access the r/Sauna subreddit and get the latest posts
subreddit = reddit.subreddit("Sauna")

# List to store each post's data
posts_data = []

# Fetch posts and add to list
for post in subreddit.new(limit=100):  # Adjust limit as needed
    post_info = {
        "title": post.title,
        "score": post.score,
        "upvotes": post.upvotes,
        "upvote_ratio": post.upvote_ratio,
        "link_flair_text": post.link_flair_text,
        "author": str(post.author),  # Convert author to string
        "num_comments": post.num_comments,
        "permalink": post.permalink,
        "url": post.url,
        "selftext": post.selftext,
        "id": post.id,
        "subreddit": post.subreddit.display_name,
        "imageurl": get_image_url(post),  # ✅ Updated to correctly extract image
        "created": post.created_utc
    }
    posts_data.append(post_info)

# Convert list of dictionaries to DataFrame
df1 = pd.DataFrame(posts_data)
df = pd.DataFrame(posts_data)

data_full = pd.concat([df, df1], axis=0, ignore_index=True)

data = data_full.drop_duplicates(subset='id', keep='first')

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
    df = data
    append_to_supabase(df, supabase_client)

