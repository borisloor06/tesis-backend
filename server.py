from gevent.pywsgi import WSGIServer
from werkzeug.middleware.proxy_fix import ProxyFix
from flask import Flask, request, jsonify
from flask_caching import Cache
import pandas as pd
import requests
from src.dbConnection.dbConnection import db_client
from src.saveDbDataFunctions.functions import get_subreddit_posts
from src.getDbDataFunctions.getMongoData import (
    getDataUnclean,
    getAnalisis
)
from src.cleanDataFunctions.cleanData import cleanData
from src.nlpAnalizeFunctions.modelBERT import SentimentAnalyzer
from src.saveDbDataFunctions.saveAnalisis import saveToDB
from src.nlpAnalizeFunctions.textFunctions import (
    TemporalAnalysis,
    AuthorAnalysis,
    CommentPostRelationship,
    KeywordIdentification,
    TopicExtraction,
    SentimentAnalysis,
)
import time
from flask_cors import CORS


def create_app():
    app = Flask(__name__)
    CORS(app)
    app.config.from_pyfile('settings.py')
    # Use ProxyFix middleware to handle reverse proxy headers
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    # Define the db_client instance as a singleton
    if not hasattr(app, "db_client"):
        app.db = db_client(app)
    return app


cache = Cache()
app = create_app()
cache.init_app(app, config={'CACHE_TYPE': 'SimpleCache'})

@app.route("/", methods=["GET"])
def home():
    return "<h1>API</h1><p>This site is a prototype API for analyzing Reddit data.</p>"


@app.route("/subreddit", methods=["GET"])
async def get_subreddit():
    start_date_str = "01-01-20 00:00:00"
    query = request.args.get("name", default="ChatGpt")
    query_comments_collection = f"{query}_comments"
    query_posts_collection = f"{query}_posts"
    posts_df, comments_df = await get_subreddit_posts(
        app, query, start_date_str, query_comments_collection, query_posts_collection
    )

    comments = comments_df.add_prefix("comments_")
    posts = posts_df.add_prefix("posts_")
    result_df = pd.merge(
        comments,
        posts,
        left_on="comments_subreddit_id",
        right_on="posts_id",
        how="inner",
    )

    result_df = result_df.to_json(orient="records", date_format="iso")

    return result_df


# @Param subreddit: Nombre del subreddit a consultar
# @Param listing: Tipo de listado a consultar (controversial, best, hot, new, random, rising, top)
# @Param limit: Cantidad de resultados a obtener
# @Param timeframe: Periodo de tiempo a consultar (hour, day, week, month, year, all)
@app.route("/reddit", methods=["GET"])
def get_reddit():
    subreddit = request.args.get("subreddit", default="ChatGPT")
    listing = request.args.get("listing", default="top")
    limit = int(request.args.get("limit", default=100))
    timeframe = request.args.get("timeframe", default="month")

    try:
        db = app.db["reddit"]
        base_url = f"https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}"
        response = requests.get(base_url, headers={"User-agent": "yourbot"})
        data = response.json()
        print(data)
        db.data.insert_many(data["data"]["children"])
        df = get_results(data)
        print(df)
        return data
    except Exception as e:
        print(e)
        return {"error": "An error occurred"}


def get_results(r):
    """
    Create a DataFrame Showing Title, URL, Score and Number of Comments.
    """
    myDict = {}
    for post in r["data"]["children"]:
        myDict[post["data"]["title"]] = {
            "url": post["data"]["url"],
            "score": post["data"]["score"],
            "comments": post["data"]["num_comments"],
        }
    df = pd.DataFrame.from_dict(myDict, orient="index")
    return df


@app.route("/gpt_data", methods=["GET"])
async def test_get_data():
    query = request.args.get("name", default="ChatGpt")
    analisis_collection = f"{query}_analisis"
    print("numero de hilos")
    # get MAX_THREADS from environment variable
    print(app.config.get("MAX_THREADS", 1))
    start_time = time.time()
    data = await getDataUnclean(app.db, query)
    print("--- %s get data seconds ---" % (time.time() - start_time))
    print("dataframe length")
    data = data.drop(columns=["comments_created_date", "posts_created_date"], axis=1)
    print(data.count())
    start_time = time.time()
    data = cleanData(data)
    print("--- %s clean seconds ---" % (time.time() - start_time))
    # eliminar columnas incompletas comments_created_date y posts_created_date
    sentiment_analyzer = SentimentAnalyzer(
        max_threads=int(app.config.get("MAX_THREADS", 1))
    )  # You can adjust the number of threads as needed
    df_sentiment = sentiment_analyzer.analyze_sentiments(
        data, text_column="comments_body"
    )
    print("--- %s sentiment analisis seconds ---" % (time.time() - start_time))

    start_time = time.time()
    author_analyzer = AuthorAnalysis(data, "comments_author", "comments_body")
    df_author = author_analyzer.analyze_author_patterns()
    print("--- %s author analisis seconds ---" % (time.time() - start_time))

    start_time = time.time()
    keyword_identifier = KeywordIdentification(data, "posts_title")
    posts_keywords = keyword_identifier.identify_keywords()
    print("--- %s post title keyword analisis seconds ---" % (time.time() - start_time))

    start_time = time.time()
    keyword_identifier = KeywordIdentification(data, "comments_body")
    comment_keywords = keyword_identifier.identify_keywords()
    print("--- %s comment keyword analisis seconds ---" % (time.time() - start_time))
    keywords =  comment_keywords.add(posts_keywords, fill_value=0)

    start_time = time.time()
    comment_post_relationship_analyzer = CommentPostRelationship(
        data, "comments_body", "posts_title", "comments_score"
    )
    df_relationship = comment_post_relationship_analyzer.analyze_relationships()
    print("--- %s relaciones analisis seconds ---" % (time.time() - start_time))

    start_time = time.time()
    topic_extractor = TopicExtraction(data, "comments_body")
    df_topic = topic_extractor.extract_topics()
    print("--- %s topic analisis seconds ---" % (time.time() - start_time))

    start_time = time.time()
    sentiment_analyzer = SentimentAnalysis(data, "comments_body")
    df_vader_sentiment = sentiment_analyzer.analyze_sentiments()
    print("--- %s sentiment analisis varder seconds ---" % (time.time() - start_time))

    dataframe = pd.DataFrame()
    dataframe["average"] = df_vader_sentiment["sentiment_score"].mean()
    dataframe["label"] = dataframe["average"].apply(
            lambda score: "positive" if score > 0 else ("neutral" if score == 0 else "negative")
        )

    print(data["comments_id"].nunique())
    print(data["posts_id"].nunique())
    print(data["comments_author"].nunique())
    print(data["comments_id"].count())
    print(data["posts_id"].count())	
    print(data["comments_author"].count())

    result_dict = {
        "average_comment_post_count": df_relationship["comment_post_count"].mean(),
        "average_comment_score": data["comments_score"].mean(),
        "average_author_comment_count": df_author["author_comment_count"].mean(),
        "total_comments": data["comments_id"].nunique(),
        "total_posts":  data["posts_id"].nunique(),
        "total_authors": data["comments_author"].nunique(),
        "transformer_analysis": df_sentiment[["admiration", "amusement", "anger", "annoyance", "approval", "caring",
                                        "confusion", "curiosity", "desire", "disappointment", "disapproval",
                                        "disgust", "embarrassment", "excitement", "fear", "gratitude", "grief",
                                        "joy", "love", "nervousness", "neutral", "optimism", "pride", "realization",
                                        "relief", "remorse", "sadness", "surprise"]].mean().to_dict(),
        "keywords": {
            "posts": posts_keywords.to_dict(),
            "comment": comment_keywords.sort_values(ascending=False).head(60).to_dict(),
            "all": keywords.sort_values(ascending=False).head(60).to_dict()
        },
        "topic_extraction": df_topic["topic_string"].value_counts().to_dict(),
        "vader_analysis": {
            "average": df_vader_sentiment[["sentiment_score"]].mean().to_dict(),
            "labels": df_vader_sentiment["sentiment_label"].value_counts().to_dict(),
        }
    }

    # Convertir el resultado a un nuevo DataFrame
    result_dataframe = pd.DataFrame([result_dict])

    saveToDB(result_dataframe, app.db, analisis_collection)
    # # save data to file
    # data.to_csv("data.csv", index=False, encoding="utf-8")
    return jsonify({"message": "ok"})


@app.route("/analisis", methods=["GET"])
@cache.cached()
async def get_analisis_data():
    query = request.args.get("name", default="ChatGpt")
    analisis_collection = f"{query}_analisis"
    comments_collection = f"{query}_comments"
    posts_collection = f"{query}_posts"
    start_time = time.time()
    analisis = await getAnalisis(app.db, analisis_collection)
    print("--- %s get analisis seconds ---" % (time.time() - start_time))
    print("-------------------analisis-------------------")
    analisis = pd.DataFrame(analisis)
    print(analisis.head(5))

    return analisis.to_json(orient="records")


@app.route("/sentiment_analisis", methods=["GET"])
@cache.cached()
async def get_analisis_sentimientos():
    query = request.args.get("name", default="ChatGpt")
    start_time = time.time()
    data = await getDataUnclean(app.db, query)

    print("--- %s get data seconds ---" % (time.time() - start_time))

    start_time = time.time()
    data = cleanData(data)
    print("--- %s clean seconds ---" % (time.time() - start_time))

    start_time = time.time()
    sentiment_analyzer = SentimentAnalysis(data, "comments_body")
    df_vader_sentiment = sentiment_analyzer.analyze_sentiments()
    print("--- %s sentiment analisis varder seconds ---" % (time.time() - start_time))
    vader_sentiment = df_vader_sentiment.to_json(orient="records")
    return vader_sentiment


@app.route("/author_analisis", methods=["GET"])
@cache.cached()
async def get_author_analisis():
    query = request.args.get("name", default="ChatGpt")
    data = await getDataUnclean(app.db, query)

    data = cleanData(data)
    author_analyzer = AuthorAnalysis(data, "comments_author", "comments_body")
    df_author = author_analyzer.analyze_author_patterns()
    author_analisis = df_author.to_json(orient="records")
    return author_analisis


@app.route("/temporal_analisis", methods=["GET"])
@cache.cached()
async def get_temporal_analisis():
    query = request.args.get("name", default="ChatGpt")
    data = await getDataUnclean(app.db, query)
    data = cleanData(data)
    temporal_analyzer = TemporalAnalysis(data, "posts_created", "comments_subreddit")
    df_time = temporal_analyzer.analyze_temporal_patterns()
    temporal_analisis = df_time.to_json(orient="records")
    return temporal_analisis


@app.route("/comment_post_relationship_analisis", methods=["GET"])
@cache.cached()
async def get_comment_post_relationship_analisis():
    query = request.args.get("name", default="ChatGpt")
    data = await getDataUnclean(app.db, query)
    data = cleanData(data)
    comment_post_relationship_analyzer = CommentPostRelationship(
        data, "comments_body", "posts_title", "comments_score"
    )
    df_relationship = comment_post_relationship_analyzer.analyze_relationships()
    comment_post_relationship_analisis = df_relationship.to_json(orient="records")
    return comment_post_relationship_analisis


@app.route("/keyword_identification", methods=["GET"])
@cache.cached()
async def get_keyword_identification():
    query = request.args.get("name", default="ChatGpt")
    data = await getDataUnclean(app.db, query)
    data = cleanData(data)
    keyword_identifier = KeywordIdentification(data, "comments_body")
    df_keyword = keyword_identifier.identify_keywords()
    keyword_identification = df_keyword.to_json(orient="records")
    return keyword_identification


@app.route("/topic_extraction", methods=["GET"])
@cache.cached()
async def get_topic_extraction():
    query = request.args.get("name", default="ChatGpt")
    data = await getDataUnclean(app.db, query)
    data = cleanData(data)
    topic_extractor = TopicExtraction(data, "comments_body")
    df_topic = topic_extractor.extract_topics()
    topic_extraction = df_topic.to_json(orient="records")
    return topic_extraction


def run_gevent_server():
    http_server = WSGIServer(("127.0.0.1", 8000), app)
    http_server.serve_forever()


if __name__ == "__main__":
    if app.config["DEBUG"]:
        app.run()
    else:
        # If the script is imported as a module, use Gevent WSGI server
        run_gevent_server()
