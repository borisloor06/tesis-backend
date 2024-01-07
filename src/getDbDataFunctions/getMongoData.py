import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
from src.cleanDataFunctions.cleanData import cleanData

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
    ).batch_size(2000)
    return list(cursor)


def getCommentsByDate(db, comments_collection_name="ChatGpt_comments", dateStart="2023-01-01", dateEnd="2023-01-31"):
    dateStartUtc = int(pd.to_datetime(dateStart, utc=True, dayfirst=True).timestamp())
    dateEndUtc = int(pd.to_datetime(dateEnd, utc=True, dayfirst=True).timestamp())
    cursor = db[comments_collection_name].find({
        "created": {
            "$gte": dateStartUtc,
            "$lt": dateEndUtc
        }
    },
    {
        "_id": 0,
    },
    ).batch_size(2000)
    return list(cursor)

def getPostsByDate(db, posts_collection_name="ChatGpt_posts", dateStart="2023-01-01", dateEnd="2023-01-31"):
    dateStartUtc = int(pd.to_datetime(dateStart, utc=True, dayfirst=True).timestamp())
    dateEndUtc = int(pd.to_datetime(dateEnd, utc=True, dayfirst=True).timestamp())
    cursor = db[posts_collection_name].find({
        "created": {
            "$gte": dateStartUtc,
            "$lt": dateEndUtc
        }
    },
    {
        "_id": 0,
    },
    ).batch_size(2000)
    return list(cursor)

def getCommentsAndPostByDateClean(db, comments_collection_name, posts_collection_name, dateStart="2023-01-01", dateEnd="2023-01-31"):
    comments = getCommentsByDate(db, comments_collection_name, dateStart, dateEnd)
    comments = pd.DataFrame(comments)
    comments[['created']] = comments[['created']].astype(object).where(comments[['created']].notnull(), None)
    posts = getPostsByDate(db, posts_collection_name, dateStart, dateEnd)
    posts = pd.DataFrame(posts)
    posts[['created']] = posts[['created']].astype(object).where(posts[['created']].notnull(), None)
    comments = comments.add_prefix("comments_")
    posts = posts.add_prefix("posts_")
    print(posts.head(5))
    result_df = pd.merge(
        comments,
        posts,
        left_on="comments_subreddit_id",
        right_on="posts_id",
        how="inner",
    )
    data = cleanData(result_df)
    return data

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

async def getData(db, query):
    comments_collection = f'{query}_comments'
    posts_collection = f'{query}_posts'
    comentarios = db[comments_collection].count_documents({})
    posts = db[posts_collection].count_documents({})
    print("------------------------COMENTARIOS-----------------------------")
    print(comentarios, posts)
    comments = await getComments(db, comments_collection)
    posts = await getPost(db, posts_collection)
    comments = pd.DataFrame(comments)
    posts = pd.DataFrame(posts)
    comments = comments.add_prefix("comments_")
    posts = posts.add_prefix("posts_")
    data = pd.merge(
        comments,
        posts,
        left_on="comments_subreddit_id",
        right_on="posts_id",
        how="inner",
    )
    data = data.drop(columns=["comments_created_date", "posts_created_date"], axis=1)
    data = cleanData(data)
    return data