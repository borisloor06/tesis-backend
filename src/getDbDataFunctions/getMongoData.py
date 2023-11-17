import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache


@lru_cache()
async def getComments(db, comments_collection_name="reddit_comments"):
    cursor = db[comments_collection_name].find({}, {"_id": 0}).batch_size(1000)
    return list(cursor)


@lru_cache()
async def getPost(db, posts_collection_name="reddit_posts"):
    cursor = db[posts_collection_name].find({}, {"_id": 0}).batch_size(1000)
    return list(cursor)


async def joinPostWithComments(
    db, comments_collection_name="reddit_comments", posts_collection_name="reddit_posts"
):
    data = await getCommentsAndPostConcurrent(
        db,
        comments_collection_name,
        posts_collection_name,
    )
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
async def getCommentsAndPostConcurrent(
    db, comments_collection_name="reddit_comments", posts_collection_name="reddit_posts"
):
    with ThreadPoolExecutor() as executor:
        comments_future = executor.submit(getComments, db, comments_collection_name)
        posts_future = executor.submit(getPost, db, posts_collection_name)

        comments = await asyncio.to_thread(comments_future.result)
        posts = await asyncio.to_thread(posts_future.result)
    return [comments, posts]
