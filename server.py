from flask import Flask, jsonify, request
import snscrape.modules.twitter as sntwitter

app = Flask(__name__)

@app.route('/tweets', methods=['GET'])
def get_tweets():
    # Obtener el tema específico de la consulta
    topic = request.args.get('topic')
    limit = request.args.get('topic')
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


if __name__ == '__main__':
    app.run()
