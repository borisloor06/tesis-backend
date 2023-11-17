def saveToDB(data, db, analisis_collection_name='analisis'):
  for doc in data.to_dict(orient='records'):
    db[analisis_collection_name].update_one(
      {'commet_id': doc['commet_id'], 'post_id': doc['post_id']},
      {'$set': doc},
      upsert=True
    )
