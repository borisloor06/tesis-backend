import pandas as pd
import datetime
from src.dbConnection.dbConnection import db_client
import asyncpraw
from aiohttp import ClientSession


# TODO refactorizar para separar logica en funciones mas pequeñas
async def get_subreddit_posts(subreddit_name, start_date_str, comments_collection_name='reddit_comments', posts_collection_name='reddit_posts'):
    # Initialize PRAW
    session = ClientSession(trust_env=True)
    reddit = asyncpraw.Reddit(requestor_kwargs={"session": session})
    db = await db_client()
    start_date = datetime.datetime.strptime(start_date_str, '%d-%m-%y %H:%M:%S').timestamp()
    subreddit = await reddit.subreddit(subreddit_name, fetch=True)
    subreddits = subreddit.top(time_filter="all", limit=None)
    comments = []
    posts = []
    i = 0
    comment_columns = ['body', 'score', 'id', 'subreddit', 'created', 'subreddit_id','author']
    post_columns = ['title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'selftext', 'created', 'created_utc', 'author', 'upvote_ratio']
    async for post in subreddits:
        date = post.created_utc
        submission = await reddit.submission(id=post.id, fetch=True)
        await submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            author = 'sin autor'
            try:
                author = comment.author.name
            except AttributeError:
                continue
            data = [comment.body, comment.score, comment.id, comment.subreddit.display_name, comment.created, comment._submission.id, author]
            comments.append(data)
            comment_see = dict(zip(comment_columns, data))
            if(db[comments_collection_name].find_one({'id': comment_see['id']}) is None):
                db[comments_collection_name].insert_one(comment_see)
            else:
                db[comments_collection_name].update_one({'id': comment_see['id']}, {'$set': comment_see})
            if i == 0:
                print(vars(comment))
                print('-------------------')
                print(vars(submission))
                print('-------------------')
                print(vars(post))
                i += 1
        if date > start_date:
            author = 'sin autor'
            try:
                author = comment.author.name
            except AttributeError:
                continue
            data = [post.title, post.score, post.id, post.subreddit.display_name, post.url, post.num_comments, post.selftext, post.created, post.created_utc, author, post.upvote_ratio]
            posts.append(data)
            post_see = dict(zip(post_columns, data))
            if(db[posts_collection_name].find_one({'id': post_see['id']}) is None):
                try:
                    db[posts_collection_name].insert_one(post_see)
                except Exception as e:
                    print(e)
                    continue
            else:
                db[posts_collection_name].update_one({'id': post_see['id']}, {'$set': post_see})
    all_posts = pd.DataFrame(posts, columns=post_columns)
    df_comments = pd.DataFrame(comments, columns=comment_columns)

    return all_posts, df_comments