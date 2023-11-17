from flask import Flask, jsonify, request
import pandas as pd
import requests
from src.dbConnection.dbConnection import db_client
from src.saveDbDataFunctions.functions import get_subreddit_posts
from src.getDbDataFunctions.getMongoData import joinPostWithComments
from src.cleanDataFunctions.cleanData import clean_reddit_data
from src.nlpAnalizeFunctions.analyzeData import analize_data, analisis_sentimientos
from src.nlpAnalizeFunctions.modelBERT import SentimentAnalyzer
from src.saveDbDataFunctions.saveAnalisis import saveToDB

def create_app():
    app = Flask(__name__)
    app.config['DEBUG'] = True
    # Define the db_client instance as a singleton
    if not hasattr(app, 'db_client'):
        app.db = db_client()
    return app

app = create_app()

@app.route('/subreddit', methods=['GET'])
async def get_subreddit():
    start_date_str = '01-01-20 00:00:00'
    query = request.args.get('name', default='ChatGpt')
    query_comments_collection = f'{query}_comments'
    query_posts_collection = f'{query}_posts'
    posts_df, comments_df = await get_subreddit_posts(query, start_date_str, query_comments_collection, query_posts_collection)
    posts_df = posts_df.to_json(orient='records', date_format='iso')
    comments_df = comments_df.to_json(orient='records')

    return jsonify(posts_df, comments_df)
#TODO Implementar el endpoint para obtener los tweets de un usuario
#TODO Guardar los tweets en una base de datos
#TODO Implementar el endpoint para obtener los tweets de un usuario desde la base de datos
#TODO Relacionar una busqueda con un usuario
#TODO Analizar los tweets de un usuario con la libreria de analisis de sentimientos
#TODO Algoritmo de limpieza de datos que funcione siempre


# @Param subreddit: Nombre del subreddit a consultar
# @Param listing: Tipo de listado a consultar (controversial, best, hot, new, random, rising, top)
# @Param limit: Cantidad de resultados a obtener
# @Param timeframe: Periodo de tiempo a consultar (hour, day, week, month, year, all)
@app.route('/reddit', methods=['GET'])
def get_reddit():
    subreddit = request.args.get('subreddit', default='ChatGPT')
    listing = request.args.get('listing', default='top')
    limit = int(request.args.get('limit', default=100))
    timeframe = request.args.get('timeframe', default='month')

    try:
        db = db_client().reddit
        base_url = f'https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}'
        response = requests.get(base_url, headers={'User-agent': 'yourbot'})
        data = response.json()
        print(data)
        db.data.insert_many(data['data']['children'])
        df = get_results(data)
        print(df)
        return jsonify(data)
    except Exception as e:
        print(e)
        return jsonify({'error': 'An error occurred'})

def get_results(r):
    '''
    Create a DataFrame Showing Title, URL, Score and Number of Comments.
    '''
    myDict = {}
    for post in r['data']['children']:
        myDict[post['data']['title']] = {'url':post['data']['url'],'score':post['data']['score'],'comments':post['data']['num_comments']}
    df = pd.DataFrame.from_dict(myDict, orient='index')
    return df

@app.route('/gpt_data', methods=['GET'])
async def test_get_data():
    data = await joinPostWithComments(app)
    columns = data.columns
    for column in columns:
        data = clean_reddit_data(data, column)
    print("-------------------columns-------------------")
    print(columns)
    data = data.head(5)
    sentiment_analyzer = SentimentAnalyzer(max_threads=4)  # You can adjust the number of threads as needed
    data = sentiment_analyzer.analyze_sentiments(data, text_column='comments_body')
    # save data to db
    saveToDB(data, app.db)

    print("-------------------data-------------------")
    print(data.head(5))
    # save data to file
    return jsonify({'message': 'ok'})
    data.to_csv('data.csv', index=False, encoding='utf-8')
    # return ok
    data = data.to_json(orient='records')
    return jsonify(data)

if __name__ == '__main__':
    app.run(port=80)
