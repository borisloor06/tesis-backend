import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

columns_used = [
    "comments_body",
    "comments_id",
    "posts_id",
    "posts_created",
    "comments_subreddit",
    "comments_author",
    "comments_score",
    "posts_title",
]


@lru_cache()
async def getComments(db, comments_collection_name="reddit_comments"):
    cursor = db[comments_collection_name].find(
        {},
        {
            "body": 1,
            "id": 1,
            "subreddit": 1,
            "author": 1,
            "score": 1,
            "subreddit_id": 1,
            "_id": 0,
        },
    )
    return list(cursor)


@lru_cache()
async def getPost(db, posts_collection_name="reddit_posts"):
    cursor = db[posts_collection_name].find(
        {}, {"id": 1, "created": 1, "title": 1, "_id": 0}
    )
    return list(cursor)


@lru_cache()
async def getAnalisis(db, posts_collection_name="ChatGpt_analisis"):
    with ThreadPoolExecutor() as executor:
        future = executor.submit(db[posts_collection_name].find, {}, {"_id": 0})
        cursor = await asyncio.to_thread(future.result)
    return cursor

async def joinPostWithComments(
    db, comments_collection_name="reddit_comments", posts_collection_name="reddit_posts"
):
    data = await getCommentsAndPostConcurrent(
        db,
        comments_collection_name,
        posts_collection_name,
    )
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
async def getCommentsAndPostConcurrent(
    db, comments_collection_name="reddit_comments", posts_collection_name="reddit_posts"
):
    with ThreadPoolExecutor() as executor:
        comments_future = executor.submit(getComments, db, comments_collection_name)
        posts_future = executor.submit(getPost, db, posts_collection_name)

        comments = await asyncio.to_thread(comments_future.result)
        posts = await asyncio.to_thread(posts_future.result)
    return [comments, posts]
