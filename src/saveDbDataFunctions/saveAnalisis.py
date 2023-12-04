def saveToDB(data, db, analisis_collection_name='analisis'):
  for doc in data.to_dict(orient='records'):
    db[analisis_collection_name].update_one(
      {},
      {'$set': doc},
      upsert=True
    )
