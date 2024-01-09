import pandas as pd
from src.cleanDataFunctions.cleanData import cleanData
import datetime

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


def getComments(db, comments_collection_name="reddit_comments"):
    cursor = db[comments_collection_name].find(
        {},
        {
            "_id": 0,
        },
    ).batch_size(2000)
    return list(cursor)

def getCommentsByLimit(db, comments_collection_name="reddit_comments", limit=1000, offset=0):
    limit = int(limit)
    offset = int(offset)
    print(limit, offset)
    cursor = db[comments_collection_name].find(
        {},
        {
            "_id": 0,
        },
    ).limit(limit).skip(offset).sort([("_id", -1)]).batch_size(2000)

    return list(cursor)

def updateDate(db, comments_collection_name="reddit_comments", posts_collection_name="reddit_posts"):
    comments = db[comments_collection_name].find({}, {"_id": 0}).batch_size(2000)
    for comment in comments:
        created_date = datetime.datetime.utcfromtimestamp(comment["created"]).strftime('%Y-%m-%d %H:%M:%S')

        db[comments_collection_name].update_one(
            {"id": comment["id"]},
            {"$set": {"created_date": created_date}},
        )
    posts = db[posts_collection_name].find({}, {"_id": 0}).batch_size(2000)
    for post in posts:
        created_date = datetime.datetime.utcfromtimestamp(post["created"]).strftime('%Y-%m-%d %H:%M:%S')
        db[posts_collection_name].update_one(
            {"id": post["id"]},
            {"$set": {"created_date": created_date}},
        )

    return True

def getPostsByLimit(db, posts_collection_name="reddit_posts", limit=1000, offset=0):
    limit = int(limit)
    offset = int(offset)
    print(limit, offset)
    cursor = db[posts_collection_name].find(
        {},
        {
            "_id": 0,
        },
    ).limit(limit).skip(offset).sort([("_id", -1)]).batch_size(2000)

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

def getPost(db, posts_collection_name="reddit_posts"):
    cursor = db[posts_collection_name].find(
        {}, {"_id": 0}
    ).batch_size(2000)
    return list(cursor)


def getAnalisis(db, posts_collection_name="ChatGpt_analisis"):
    cursor = db[posts_collection_name].find({}, {"_id": 0}).batch_size(2000)
    return list(cursor)

def updateAnalisis(db, posts_collection_name="ChatGpt_analisis", data={}):
    db[posts_collection_name].find_one_and_update(
        {},
        {"$set": data},
    )
    return True


def getData(db, query):
    comments_collection = f'{query}_comments'
    posts_collection = f'{query}_posts'
    comentarios = db[comments_collection].count_documents({})
    posts = db[posts_collection].count_documents({})
    print("------------------------COMENTARIOS-----------------------------")
    print(comentarios, posts)
    comments = getComments(db, comments_collection)
    posts = getPost(db, posts_collection)
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

def getCommentsAndPost(db, query):
    comments_collection = f'{query}_comments'
    posts_collection = f'{query}_posts'
    comments = getComments(db, comments_collection)
    posts = getPost(db, posts_collection)
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