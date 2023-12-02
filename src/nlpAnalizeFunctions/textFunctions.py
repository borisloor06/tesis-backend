from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
nltk.download('vader_lexicon')

class TemporalAnalysis:
    def __init__(self, dataframe, time_column, subreddit_column):
        self.dataframe = dataframe
        self.time_column = time_column
        self.subreddit_column = subreddit_column

    def analyze_temporal_patterns(self):
        temporal_data = (
            self.dataframe.groupby(
                [
                    self.subreddit_column,
                    pd.to_datetime(self.dataframe[self.time_column]).dt.to_period("M"),
                ]
            )
        )
        temporal_data = pd.concat([dato for _, dato in temporal_data])

        # Utilizar pd.concat en lugar de pd.merge
        self.dataframe = pd.concat([
            self.dataframe,
            temporal_data
        ], axis=1)

        return self.dataframe


class AuthorAnalysis:
    def __init__(self, dataframe, author_column, text_column):
        self.dataframe = dataframe
        self.author_column = author_column
        self.text_column = text_column

    def analyze_author_patterns(self):
        author_data = (
            self.dataframe.groupby(self.author_column)
            .agg({self.text_column: "count", "comments_score": "mean"})
            .reset_index()
        )

        author_data.columns = [
            self.author_column,
            "author_comment_count",
            "average_author_comment_score",
        ]

        self.dataframe = pd.merge(
            self.dataframe, author_data, on=self.author_column, how="left"
        )

        return self.dataframe


class CommentPostRelationship:
    def __init__(self, dataframe, comment_column, post_column, score_column):
        self.dataframe = dataframe
        self.comment_column = comment_column
        self.post_column = post_column
        self.score_column = score_column

    def analyze_relationships(self):
        grouped_data = (
            self.dataframe.groupby(self.post_column)
            .agg({self.comment_column: "count", self.score_column: "mean"})
            .reset_index()
        )

        grouped_data.columns = [
            self.post_column,
            "comment_post_count",
            "average_comment_post_score",
        ]

        self.dataframe = pd.merge(
            self.dataframe, grouped_data, on=self.post_column, how="left"
        )

        return self.dataframe


class KeywordIdentification:
    def __init__(self, dataframe, text_column):
        self.dataframe = dataframe
        self.text_column = text_column

    def identify_keywords(self):
        vectorizer = CountVectorizer(stop_words="english")
        X = vectorizer.fit_transform(self.dataframe[self.text_column])

        keywords = vectorizer.get_feature_names_out()
        keyword_counts = X.sum(axis=0).A1
        keyword_df = pd.DataFrame({"keyword": keywords, "keyword_counts": keyword_counts})
        self.dataframe = pd.concat([
            self.dataframe,
            keyword_df
        ], axis=1, )

        return self.dataframe


class TopicExtraction:
    def __init__(self, dataframe, text_column):
        self.dataframe = dataframe
        self.text_column = text_column

    def extract_topics(self):
        vectorizer = CountVectorizer(stop_words="english")
        X = vectorizer.fit_transform(self.dataframe[self.text_column])

        lda = LatentDirichletAllocation(
            n_components=5, random_state=42
        )  # Puedes ajustar el número de tópicos
        topics = lda.fit_transform(X)
        # identify keywords for each topic
        # n = 30  # Puedes ajustar el número de palabras por tópico
        # for index, topic in enumerate(lda.components_):
        #     print(f'The top {n} words for topic #{index}')
        #     print([vectorizer.get_feature_names_out()[i] for i in topic.argsort()[-n:]])
        self.dataframe["topic"] = topics.argmax(axis=1)
        self.dataframe["topic_string"] = self.dataframe["topic"].map({
            0: 'Chat and Humor',
            1: 'Casual Conversations and Humor',
            2: 'Superhero and Fantasy',
            3: 'Chat and Humor',
            4: 'AI and Technology'
            # Add more mappings if you have additional topics
        })
        return self.dataframe


class SentimentAnalysis:
    def __init__(self, dataframe, text_column):
        self.dataframe = dataframe
        self.text_column = text_column

    def analyze_sentiments(self):
        sid = SentimentIntensityAnalyzer()
        self.dataframe["sentiment_score"] = self.dataframe[self.text_column].apply(
            lambda x: sid.polarity_scores(x)["compound"]
        )
        # Categorize sentiment scores into labels
        self.dataframe["sentiment_label"] = self.dataframe["sentiment_score"].apply(
            lambda score: "positive" if score > 0 else ("neutral" if score == 0 else "negative")
        )
        return self.dataframe