from functools import lru_cache
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
stop_words = set(stopwords.words('english'))

def clean_reddit_data(dataframe, column):
    # Drop duplicates if necessary
    df = dataframe.drop_duplicates()

    # Remove rows with missing values (NaN)
    df = df.dropna()

    df[column] = df[column].apply(clean_text)
    df[column] = df[column].apply(clean_data)
    df[column] = df[column].apply(delete_stopwords)

    return df

@lru_cache()
def clean_text(text):
    import re
    # validar si la columna es de tipo string
    if not validar_str(text):
        return text
    # Example: Remove special characters, HTML tags, and extra white spaces
    text = re.sub(r'<[^>]+>', ' ', text)  # Remove HTML tags
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove special characters
    text = ' '.join(text.split())  # Remove extra white spaces

    return text

@lru_cache()
def clean_data(text):
    if not validar_str(text):
        return text
    try:
        return text.encode("utf-8", "replace").decode("utf-8")
    except Exception as e:
        return "?"
    
@lru_cache()
def delete_stopwords(text):
    if not validar_str(text):
        return text
    # Usar un conjunto para las stopwords

    # Tokenizar el texto de manera más eficiente
    words = word_tokenize(text)

    # Filtrar las stopwords
    filtered_words = [word for word in words if word.lower() not in stop_words]

    # Unir las palabras filtradas en un solo texto
    filtered_text = ' '.join(filtered_words)

    return filtered_text

@lru_cache()
def validar_str(text):
    return isinstance(text, str)


def cleanData(data):
    columns = data.columns
    for column in columns:
        data = clean_reddit_data(data, column)
    return data