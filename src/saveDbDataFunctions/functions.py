import pandas as pd
import datetime
from aiohttp import ClientSession
import asyncpraw
from prawcore.exceptions import PrawcoreException
import time

# Define comment_columns and post_columns
comment_columns = ['body', 'score', 'id', 'subreddit', 'created', 'subreddit_id', 'author', 'created_date']
post_columns = ['title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'selftext', 'created', 'created_utc', 'author', 'upvote_ratio', 'created_date']

async def fetch_comments_data(comment, comments_collection_name, db):
    # Fetch and store comments data
    author = 'sin autor'
    try:
        author = comment.author.name
    except AttributeError:
        pass

    data = [comment.body, comment.score, comment.id, comment.subreddit.display_name, comment.created, comment._submission.id, author]
    comment_see = dict(zip(comment_columns[:-1], data))  # Exclude 'created_date' from keys
    comment_see['created_date'] = datetime.datetime.utcfromtimestamp(comment.created).strftime('%Y-%m-%d %H:%M:%S')

    if db[comments_collection_name].find_one({'id': comment_see['id']}) is None:
        db[comments_collection_name].insert_one(comment_see)
    else:
        db[comments_collection_name].update_one({'id': comment_see['id']}, {'$set': comment_see})

    return comment_see


async def fetch_posts_data(post, posts_collection_name, start_date, db):
    # Fetch and store posts data
    date = post.created_utc
    if date > start_date:
        author = 'sin autor'
        try:
            author = post.author.name
        except AttributeError:
            pass

        data = [post.title, post.score, post.id, post.subreddit.display_name, post.url, post.num_comments, post.selftext, post.created, post.created_utc, author, post.upvote_ratio]
        post_see = dict(zip(post_columns[:-1], data))  # Exclude 'created_date' from keys
        post_see['created_date'] = datetime.datetime.utcfromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S')

        if db[posts_collection_name].find_one({'id': post_see['id']}) is None:
            try:
                db[posts_collection_name].insert_one(post_see)
            except Exception as e:
                print(e)
        else:
            db[posts_collection_name].update_one({'id': post_see['id']}, {'$set': post_see})

        return post_see

async def get_subreddit_posts(app, subreddit_name, start_date_str, comments_collection_name='reddit_comments', posts_collection_name='reddit_posts'):
    
    session = ClientSession(trust_env=True)
    reddit = asyncpraw.Reddit(requestor_kwargs={"session": session})
    db = app.db

    start_date = datetime.datetime.strptime(start_date_str, '%d-%m-%y %H:%M:%S').timestamp()
    subreddit = await reddit.subreddit(subreddit_name, fetch=True)

    subreddits_top = subreddit.top(time_filter="all", limit=None)
    subreddits_hot = subreddit.hot(limit=None)
    # subreddits_new = subreddit.new(limit=None)
    comments = []
    posts = []

    async def fetchData(subreddits):
        async for post in subreddits:
            try:
                submission = await reddit.submission(id=post.id)
            except (asyncpraw.exceptions.APIException, PrawcoreException):
                time.sleep(30)
                submission = await reddit.submission(id=post.id)
            try:
                await submission.comments.replace_more(limit=0)
            except (asyncpraw.exceptions.APIException, PrawcoreException):
                time.sleep(30)
                await submission.comments.replace_more(limit=0)

            for comment in submission.comments.list():
                comments.append(await fetch_comments_data(comment, comments_collection_name, db))

            posts.append(await fetch_posts_data(post, posts_collection_name, start_date, db))

    await fetchData(subreddits_top)
    await fetchData(subreddits_hot)
    # await fetchData(subreddits_new)

    all_posts = pd.DataFrame(posts, columns=post_columns)
    df_comments = pd.DataFrame(comments, columns=comment_columns)

    return all_posts, df_comments