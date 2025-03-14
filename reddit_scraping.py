import praw
import pandas as pd
from supabase import create_client
import os
import mimetypes

# ✅ Supabase connection details
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ✅ Initialize Supabase client
def initialize_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ✅ Convert Unix timestamp to readable datetime format
def convert_timestamp(unix_timestamp):
    return datetime.utcfromtimestamp(float(unix_timestamp)).strftime('%Y-%m-%d %H:%M:%S')

# ✅ Get existing post and comment IDs from Supabase
def get_existing_ids(supabase_client, table_name, column_name):
    response = supabase_client.table(table_name).select(column_name).execute()
    if response.data:
        return set(row[column_name] for row in response.data)
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

# ✅ Get all existing post & comment IDs
existing_post_ids = get_existing_ids(supabase_client, "scraping_table", "id")
existing_comment_ids = get_existing_ids(supabase_client, "reddit_sauna_comments", "id")

# ✅ Fetch new posts from Reddit
posts_data = []
comments_data = []

print("🚀 Fetching new posts...")
for post in subreddit.new(limit=100):  # Adjust limit as needed
    if post.id not in existing_post_ids:  # ✅ Avoid inserting duplicates
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
            "created": convert_timestamp(post.created_utc)  # ✅ Store as readable datetime
        }
        posts_data.append(post_info)

print(f"✅ Total new posts scraped: {len(posts_data)}")

# ✅ Fetch and store comments for all posts (new and existing)
post_ids = get_existing_ids(supabase_client, "scraping_table", "id")

print("🚀 Fetching comments for all posts...")
for post_id in post_ids:
    submission = reddit.submission(id=post_id)
    submission.comments.replace_more(limit=0)  # Expand comments fully

    print(f"🔍 Fetching comments for post ID: {post_id}")
    for comment in submission.comments.list():
        if comment.id not in existing_comment_ids:  # ✅ Avoid inserting duplicates
            comment_info = {
                "id": comment.id,
                "post_id": post_id,
                "author": str(comment.author) if comment.author else "[deleted]",
                "body": comment.body,
                "score": comment.score,
                "created": convert_timestamp(comment.created_utc)  # ✅ Store as readable datetime
            }
            comments_data.append(comment_info)

print(f"✅ Total new comments scraped: {len(comments_data)}")

# ✅ Insert both posts and comments into Supabase
def append_to_supabase(posts, comments, supabase_client):
    if posts:
        print(f"🚀 Inserting {len(posts)} new posts...")
        post_response = supabase_client.table("scraping_table").insert(posts).execute()
        if post_response.data:
            print(f"✅ Inserted {len(post_response.data)} posts.")
        else:
            print("⚠️ Error inserting posts.")

    if comments:
        print(f"🚀 Inserting {len(comments)} new comments...")
        comment_response = supabase_client.table("reddit_sauna_comments").insert(comments).execute()
        if comment_response.data:
            print(f"✅ Inserted {len(comment_response.data)} comments.")
        else:
            print("⚠️ Error inserting comments.")

# ✅ Run the script
if __name__ == "__main__":
    append_to_supabase(posts_data, comments_data, supabase_client)
    print("✅ Script finished running.")
