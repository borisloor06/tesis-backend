import pandas as pd
from dbConnection import db_client

async def getComments():
  db = await db_client()
  comments = db.reddit_comments.find({})
  df_comments = pd.DataFrame(list(comments))
  df_comments.head(5)
  return df_comments
async def getPost():
  db = await db_client()
  post = db.reddit_posts.find({})
  df_post = pd.DataFrame(list(post))
  df_post.head(5)
  return df_post

async def joinPostWithComments():
  comments = getComments()
  posts = getPost()
  result_df = pd.merge(comments, posts, left_on="subreddit_id", right_on="id", how="inner")

  test_group = comments.groupby(["subreddit_id"])
  # result_df = pd.concat([comments, posts], axis=1)

  return result_df