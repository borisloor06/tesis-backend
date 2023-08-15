import pandas as pd
import datetime
from dbConnection import db_client
import asyncpraw
from aiohttp import ClientSession



async def get_subreddit_posts(subreddit_name, start_date_str):
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
    async for post in subreddits:
        date = post.created_utc
        submission = await reddit.submission(id=post.id, fetch=True)
        await submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            columns = ['body', 'score', 'id', 'subreddit', 'created', 'author']
            author = 'sin autor'
            try:
                author = comment.author.name
            except AttributeError:
                continue
            data = [comment.body, comment.score, comment.id, comment.subreddit.display_name, comment.created, author]
            comments.append(data)
            comment_see = dict(zip(columns, data))
            print(comment_see)
            if(db.reddit_comments.find_one({'id': comment_see['id']}) is None):
                db.reddit_comments.insert_one(comment_see)
            if i == 0:
                print(vars(comment))
                print(vars(post))
                i += 1
        if date > start_date:
            columns = ['title', 'score', 'id', 'url', 'num_comments', 'selftext', 'created', 'created_utc', 'author']
            author = 'sin autor'
            try:
                author = comment.author.name
            except AttributeError:
                continue
            data = [post.title, post.score, post.id, post.url, post.num_comments, post.selftext, post.created, post.created_utc, author]
            posts.append(data)
            post_see = dict(zip(columns, data))
            print(post_see)
            if(db.reddit_posts.find_one({'id': post_see['id']}) is None):
                db.reddit_posts.insert_one(post_see)
    all_posts = pd.DataFrame(posts, columns=['title', 'score', 'id', 'url', 'num_comments', 'selftext', 'created', 'created_utc', 'author'])
    all_posts['created_utc'] = pd.to_datetime(all_posts['Created On'], unit='s')
    df_comments = pd.DataFrame(comments, columns=['body', 'score', 'id', 'created',  'author'])

    return all_posts, df_comments

    posts = []
    comments = []

    for post in s_reddit:
        submission = reddit.submission(id=post.id)
        submission.comments.replace_more(limit=0)
        for comment in submission.comments.list():
            comments.append([comment.body, comment.score, comment.id, comment.subreddit, comment.url, comment.num_comments, comment.selftext, comment.created, comment.author])
        posts.append([post.title, post.score, post.id, post.subreddit, post.url, post.num_comments, post.selftext, post.created, post.author, post.upvote_ratio, post.view_count, post.all_awardings, post.awarders, post.link_flair_text, post.link_flair_type, post.locked, post.media, post.media_embed, post.num_crossposts, post.pinned, post.stickied, post.subreddit_subscribers, post.thumbnail])

    df_posts = pd.DataFrame(posts, columns=['title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'selftext', 'created', 'author', 'upvote_ratio', 'view_count', 'all_awardings', 'awarders', 'link_flair_text', 'link_flair_type', 'locked', 'media', 'media_embed', 'num_crossposts', 'pinned', 'stickied', 'subreddit_subscribers', 'thumbnail'])
    df_comments = pd.DataFrame(comments, columns=['body', 'score', 'id', 'subreddit', 'url', 'num_comments', 'selftext', 'created', 'author'])

    return df_posts, df_comments