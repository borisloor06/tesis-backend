def saveToDB(data, db, analisis_collection_name='analisis'):
  for doc in data.to_dict(orient='records'):
    db[analisis_collection_name].update_one(
      {'comments_id': doc['comments_id'], 'posts_id': doc['posts_id']},
      {'$set': doc},
      upsert=True
    )
