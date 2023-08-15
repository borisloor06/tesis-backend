from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

async def db_client():
    # Replace the placeholder with your Atlas connection string
    uri = "mongodb+srv://scrapper:Z5zP20DlS04dpmQF@myatlasclusteredu.anyvuhn.mongodb.net/?retryWrites=true&w=majority"

    # Set the Stable API version when creating a new client
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return client.data
    except Exception as e:
        print(e)
        print("Unable to connect to MongoDB. Check your connection URI.")
        return None
    
