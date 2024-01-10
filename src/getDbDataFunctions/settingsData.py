
def getConfig(db):
    config = db.config.find_one({}, {'_id': 0})
    return config

def createConfig(db, config):
    db.config.update_one({}, {'$set': config}, upsert=True)
    return config
