import pandas as pd
from textblob import TextBlob
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report


# Descargar recursos adicionales para NLTK
import nltk
nltk.download('stopwords')
nltk.download('punkt')

def analyze_sentiments(data):
    data['sentiment'] = data['comments_body'].apply(lambda x: analyze_sentiment(x))
    return data

def analyze_sentiment(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity < 0:
        return 'Negative'
    else:
        return 'Neutral'


def classify_text(data):
    # Prepare the text data
    X = data['comments_body']
    y = data['category']

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Create a TF-IDF vectorizer
    tfidf_vectorizer = TfidfVectorizer(max_features=5000, stop_words=stopwords.words('english'))

    # Transform text data into TF-IDF features
    X_train_tfidf = tfidf_vectorizer.fit_transform(X_train)
    X_test_tfidf = tfidf_vectorizer.transform(X_test)

    # Train a Multinomial Naive Bayes classifier
    classifier = MultinomialNB()
    classifier.fit(X_train_tfidf, y_train)

    # Predict the categories for the test data
    y_pred = classifier.predict(X_test_tfidf)

    # Evaluate the model
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    return accuracy, report

def analize_data(data):
    data = assign_categories(data)
    data = analyze_sentiments(data)
    accuracy, report = classify_text(data)
    return accuracy, report

# Función para asignar categorías a los textos
def assign_categories(data):
    # Preprocesar el texto: tokenización y eliminación de stopwords
    stop_words = set(stopwords.words('english'))
    tokenized_data = []

    for text in data['comments_body']:
        words = word_tokenize(text)
        words = [word.lower() for word in words if word.isalnum() and word.lower() not in stop_words]
        tokenized_data.append(words)

    # Lista de palabras clave para categorías
    category_keywords = {
        'sports': ['football', 'basketball', 'tennis', 'soccer'],
        'technology': ['technology', 'gadget', 'software', 'innovation'],
        'food': ['food', 'cooking', 'recipe', 'restaurant'],
        'music': ['music', 'song', 'artist', 'concert'],
        'travel': ['travel', 'destination', 'adventure', 'vacation']
        # Agrega más categorías y palabras clave según tus necesidades
    }

    # Asignar categorías en función de palabras clave
    assigned_categories = []
    for text in tokenized_data:
        category_found = False
        for category, keywords in category_keywords.items():
            if any(keyword in text for keyword in keywords):
                assigned_categories.append(category)
                category_found = True
                break
        if not category_found:
            assigned_categories.append('other')  # Categoría por defecto

    data['category'] = assigned_categories
    return data