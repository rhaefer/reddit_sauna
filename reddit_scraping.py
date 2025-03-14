import praw
import pandas as pd
from supabase import create_client, Client
import os
import mimetypes

# ✅ Supabase connection details
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ✅ Initialize Supabase client
def initialize_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ Get existing post IDs from Supabase
def get_existing_post_ids(supabase_client):
    response = supabase_client.table("scraping_table").select("id").execute()
    if response.data:
        return set(row["id"] for row in response.data)
    return set()

# ✅ Function to get image URLs
def get_image_url(post):
    if hasattr(post, "media_metadata") and post.media_metadata:
        for item in post.media_metadata.values():
            if "s" in item and "u" in item["s"]:
                return item["s"]["u"].replace("&amp;", "&")  # Fix encoding
    mime_type, _ = mimetypes.guess_type(post.url)
    if mime_type and mime_type.startswith("image"):
        return post.url
    return None

# ✅ Initialize Reddit API (PRAW)
reddit = praw.Reddit(
    client_id="hTujd0OmhH9KsGFt_jut4Q",
    client_secret="WWCqKzEDpktPGdT9-CZ8sAaV28bj2A",
    user_agent="sauna app v1.0 by /u/reidhaefer"
)

# ✅ Access subreddit
subreddit = reddit.subreddit("Sauna")

# ✅ Initialize Supabase
supabase_client = initialize_supabase()

# ✅ Get all existing post IDs to prevent inserting duplicates
existing_post_ids = get_existing_post_ids(supabase_client)

# ✅ Fetch new posts from Reddit
posts_data = []
for post in subreddit.new(limit=100):  # Adjust limit as needed
    if post.id not in existing_post_ids:  # ✅ **Avoid inserting duplicates**
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

# ✅ Convert to DataFrame and remove duplicates (before inserting)
df_new = pd.DataFrame(posts_data)
df_new = df_new.drop_duplicates(subset='id', keep='first')  # Extra safety measure

# ✅ Insert only **new unique posts** into Supabase
def append_to_supabase(df, supabase_client):
    if df.empty:
        print("✅ No new posts to insert.")
        return

    records = df.to_dict(orient='records')
    response = supabase_client.table("scraping_table").insert(records).execute()

    if response.data:
        print(f"✅ Inserted {len(response.data)} new records into Supabase.")
    else:
        print(f"⚠️ Error inserting records: {response}")

# ✅ Get existing comment IDs from Supabase
def get_existing_comment_ids(supabase_client):
    response = supabase_client.table("reddit_sauna_comments").select("id").execute()
    if response.data:
        return set(row["id"] for row in response.data)
    return set()

# ✅ Get post IDs from Supabase (to scrape comments for these posts)
def get_post_ids(supabase_client):
    response = supabase_client.table("scraping_table").select("id").execute()
    if response.data:
        return [row["id"] for row in response.data]
    return []

# ✅ Get existing comments to prevent inserting duplicates
existing_comment_ids = get_existing_comment_ids(supabase_client)

# ✅ Get all post IDs from Supabase
post_ids = get_post_ids(supabase_client)

# ✅ Fetch and store comments
comments_data = []
for post_id in post_ids:
    submission = reddit.submission(id=post_id)
    submission.comments.replace_more(limit=0)  # Expand comments fully

    for comment in submission.comments.list():
        if comment.id not in existing_comment_ids:  # ✅ Avoid inserting duplicates
            comment_info = {
                "id": comment.id,
                "post_id": post_id,
                "author": str(comment.author) if comment.author else "[deleted]",
                "body": comment.body,
                "score": comment.score,
                "created": comment.created_utc,
            }
            comments_data.append(comment_info)

# ✅ Convert to DataFrame and remove duplicates (before inserting)
df_comments = pd.DataFrame(comments_data)
df_comments = df_comments.drop_duplicates(subset="id", keep="first")  # Extra safety

# ✅ Insert only **new unique comments** into Supabase
def append_to_supabase2(df, supabase_client):
    if df.empty:
        print("✅ No new comments to insert.")
        return

    records = df.to_dict(orient="records")
    response = supabase_client.table("reddit_sauna_comments").insert(records).execute()

    if response.data:
        print(f"✅ Inserted {len(response.data)} new comments into Supabase.")
    else:
        print(f"⚠️ Error inserting comments: {response}")
        
# ✅ Run the script
if __name__ == "__main__":
    print("🚀 Running post scraper...")
    append_to_supabase(df_new, supabase_client)

    print("🚀 Running comment scraper...")
    append_to_supabase2(df_comments, supabase_client)

    print("✅ Script finished running.")
