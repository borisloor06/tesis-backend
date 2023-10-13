def clean_reddit_data(dataframe, text):
    # Drop duplicates if necessary
    df = dataframe.drop_duplicates()

    # Remove rows with missing values (NaN)
    df = df.dropna()

    # Clean text data (e.g., remove special characters, HTML tags, etc.)
    df[text] = df[text].apply(lambda x: clean_text(x))

    return df

def clean_text(text):
    # Example: Remove special characters, HTML tags, and extra white spaces
    text = re.sub(r'<[^>]+>', ' ', text)  # Remove HTML tags
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Remove special characters
    text = ' '.join(text.split())  # Remove extra white spaces

    return text