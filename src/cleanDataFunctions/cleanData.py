from functools import lru_cache

def clean_reddit_data(dataframe, column):
    # Drop duplicates if necessary
    df = dataframe.drop_duplicates()

    # Remove rows with missing values (NaN)
    df = df.dropna()

    df[column] = df[column].apply(clean_text)
    df[column] = df[column].apply(clean_data)

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
def validar_str(text):
    return isinstance(text, str)