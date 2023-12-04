import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor

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


async def getComments(db, comments_collection_name="reddit_comments"):
    cursor = db[comments_collection_name].find(
        {},
        {
            "_id": 0,
        },
    ).batch_size(1000)
    return list(cursor)


async def getPost(db, posts_collection_name="reddit_posts"):
    cursor = db[posts_collection_name].find(
        {}, {"_id": 0}
    ).batch_size(1000)
    return list(cursor)


async def getAnalisis(db, posts_collection_name="ChatGpt_analisis"):
    with ThreadPoolExecutor() as executor:
        result = await asyncio.to_thread(db[posts_collection_name].find, {}, {"_id": 0})
    return result

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


async def getCommentsAndPostConcurrent(
    db, comments_collection_name="reddit_comments", posts_collection_name="reddit_posts"
):
    with ThreadPoolExecutor() as executor:
        comments_future = executor.submit(getComments, db, comments_collection_name)
        posts_future = executor.submit(getPost, db, posts_collection_name)

        comments = await asyncio.to_thread(comments_future.result)
        posts = await asyncio.to_thread(posts_future.result)
    return [comments, posts]

async def getDataUnclean(db, query):
    comments_collection = f'{query}_comments'
    posts_collection = f'{query}_posts'
    print(db)
    comentarios = db[comments_collection].count_documents({})
    posts = db[posts_collection].count_documents({})
    print("------------------------COMENTARIOS-----------------------------")
    print(comentarios, posts)
    data = await joinPostWithComments(db, comments_collection, posts_collection)
    return data