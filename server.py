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
from src.nlpAnalizeFunctions.textFunctions import TemporalAnalysis, AuthorAnalysis, CommentPostRelationship, KeywordIdentification, TopicExtraction, SentimentAnalysis

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
    query = request.args.get('name', default='ChatGpt')
    comments_collection = f'{query}_comments'
    posts_collection = f'{query}_posts'
    analisis_collection = f'{query}_analisis'
    import time
    start_time = time.time()
    data = await joinPostWithComments(app.db, comments_collection, posts_collection)
    print("--- %s seconds ---" % (time.time() - start_time))
    columns = data.columns
    for column in columns:
        data = clean_reddit_data(data, column)
    print("-------------------columns-------------------")
    print(columns)

    # data = data.head(5)

    temporal_analyzer = TemporalAnalysis(data, 'posts_created', 'comments_subreddit')
    df_time = temporal_analyzer.analyze_temporal_patterns()

    sentiment_analyzer = SentimentAnalyzer(max_threads=16)  # You can adjust the number of threads as needed
    df_sentiment = sentiment_analyzer.analyze_sentiments(data, text_column='comments_body')
    # save data to db
    print("-------------------data-------------------")
    print(data.head(5))
    print(df_sentiment.head(5))

    author_analyzer = AuthorAnalysis(data, 'comments_author', 'comments_body')
    df_author = author_analyzer.analyze_author_patterns()
    comment_post_relationship_analyzer = CommentPostRelationship(data, 'comments_body', 'posts_title', 'comments_score')
    df_relationship = comment_post_relationship_analyzer.analyze_relationships()
    # keyword_identifier = KeywordIdentification(data, 'comments_body')
    # df_keyword = keyword_identifier.identify_keywords()
    topic_extractor = TopicExtraction(data, 'comments_body')
    df_topic = topic_extractor.extract_topics()
    sentiment_analyzer = SentimentAnalysis(data, 'comments_body')
    df_vader_sentiment = sentiment_analyzer.analyze_sentiments()

    dataframes = [data, df_sentiment, df_time, df_author, df_relationship, df_topic, df_vader_sentiment]
    df_analisis = pd.concat(dataframes, axis=1)
    print("-------------------df_analisis-------------------")
    print(df_analisis.head(5))
    print(df_analisis.columns)
    df_analisis = df_analisis.drop(columns=['comments_body', 'posts_created', 'comments_subreddit', 'comments_author', 'comments_score', 'posts_title', 'comments_subreddit_id'])
    print(df_analisis.columns)

    saveToDB(df_analisis, app.db, analisis_collection)
    # save data to file
    data.to_csv('data.csv', index=False, encoding='utf-8')
    return jsonify({'message': 'ok'})
    # return ok
    data = data.to_json(orient='records')
    return jsonify(data)

if __name__ == '__main__':
    app.run(port=80)
