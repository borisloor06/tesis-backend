import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

@lru_cache()
async def getComments(app):
  cursor = app.db.reddit_comments.find({}, {'_id': 0})
  return list(cursor)

@lru_cache()
async def getPost(app):
  cursor = app.db.reddit_posts.find({}, {'_id': 0})
  return list(cursor)


async def joinPostWithComments(app):
    data = await getCommentsAndPostConcurrent(app)
    print(data)
    comments, posts = await data[0], await data[1]
    comments = pd.DataFrame(comments)
    posts = pd.DataFrame(posts)

    comments = comments.add_prefix("comments_")
    posts = posts.add_prefix("posts_")
    result_df = pd.merge(
        comments,
        posts,
        left_on="comments_subreddit_id",
        right_on="posts_id",
        how="inner",
    )
    return result_df

@lru_cache()
async def getCommentsAndPostConcurrent(app):
    with ThreadPoolExecutor() as executor:
        comments_future = executor.submit(getComments, app)
        posts_future = executor.submit(getPost, app)

        comments = await asyncio.to_thread(comments_future.result)
        posts = await asyncio.to_thread(posts_future.result)
    return [comments, posts]