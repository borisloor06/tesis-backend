from flask import Flask, jsonify, request
import snscrape.modules.twitter as sntwitter
import pandas as pd
import requests

app = Flask(__name__)

@app.route('/tweets', methods=['GET'])
def get_tweets():
    # Obtener el tema específico de la consulta
    topic = request.args.get('topic')
    limit = request.args.get('limit')
    # Realizar la búsqueda de tweets
    tweets = []
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(f'"{topic}" lang:es').get_items()):
        if i>limit:
            break
        # Verificar si el tweet es promocionado y omitirlo
        tweets.append({
            'id': tweet.id,
            'content': tweet.rawContent,
            'user': {
                'id': tweet.user.id,
                'username': tweet.user.username
            }
        })
    print(f'Found {len(tweets)} tweets')
    print(tweets)
    return jsonify(tweets)

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
        base_url = f'https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}'
        response = requests.get(base_url, headers={'User-agent': 'yourbot'})
        data = response.json()
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


if __name__ == '__main__':
    app.run()
