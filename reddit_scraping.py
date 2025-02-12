import praw
import httpx
import pandas as pd
from supabase import create_client, Client
import os
import mimetypes

# Supabase connection details
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def get_image_url(post):
    """Extract the correct image URL from a Reddit post."""
    if hasattr(post, "media_metadata") and post.media_metadata:
        for item in post.media_metadata.values():
            if "s" in item and "u" in item["s"]:
                return item["s"]["u"].replace("&amp;", "&")  # Fix encoding
    
    mime_type, _ = mimetypes.guess_type(post.url)
    if mime_type and mime_type.startswith("image"):
        return post.url
    return None

def initialize_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ **Fetch existing post IDs from Supabase to prevent duplicates**
def get_existing_post_ids(supabase_client):
    response = supabase_client.table("scraping_table").select("id").execute()
    if response.data:
        return set(row["id"] for row in response.data)
    return set()

# Initialize PRAW (Reddit API)
reddit = praw.Reddit(
    client_id="hTujd0OmhH9KsGFt_jut4Q",
    client_secret="WWCqKzEDpktPGdT9-CZ8sAaV28bj2A",
    user_agent="sauna app v1.0 by /u/reidhaefer"
)

# Access the r/Sauna subreddit
subreddit = reddit.subreddit("Sauna")

# Initialize Supabase client
supabase_client = initialize_supabase()

# ✅ Get existing post IDs
existing_post_ids = get_existing_post_ids(supabase_client)

# ✅ List to store new posts
posts_data = []

# ✅ Fetch new posts
for post in subreddit.new(limit=100):  # Adjust limit as needed
    if post.id not in existing_post_ids:  # ✅ **Avoid duplicates**
        post_info = {
            "title": post.title,
            "score": post.score,
            "upvote_ratio": post.upvote_ratio,
            "link_flair_text": post.link_flair_text,
            "author": str(post.author) if post.author else "[deleted]",
            "num_comments": post.num_comments,
            "permalink": post.permalink,
            "url": post.url,
            "selftext": post.selftext,
            "id": post.id,
            "subreddit": post.subreddit.display_name,
            "imageurl": get_image_url(post),
            "created": post.created_utc
        }
        posts_data.append(post_info)

# ✅ Convert to DataFrame and remove duplicates
df = pd.DataFrame(posts_data)
df = df.drop_duplicates(subset='id', keep='first')

# ✅ Insert only **new unique posts** into Supabase
def append_to_supabase(df, supabase_client):
    if df.empty:
        print("✅ No new posts to insert.")
        return
    
    data_records = df.to_dict(orient='records')  # Convert DataFrame to list of dictionaries
    response = supabase_client.table("scraping_table").insert(data_records).execute()

    if response.data:
        print(f"✅ Inserted {len(response.data)} new records into Supabase.")
    else:
        print(f"⚠️ Error inserting records: {response}")

# ✅ Run script
if __name__ == "__main__":
    append_to_supabase(df, supabase_client)
