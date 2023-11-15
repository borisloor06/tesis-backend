
# Para aprovechar al máximo los recursos y obtener el mejor rendimiento posible, puedes implementar paralelismo y procesamiento en lotes (batch processing) en tu código. Esto te permitirá procesar múltiples textos simultáneamente y acelerar el análisis de sentimientos. Además, puedes utilizar la biblioteca concurrent.futures para realizar el procesamiento en paralelo. Aquí tienes una versión mejorada del código:

import torch
import pandas as pd
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from concurrent.futures import ThreadPoolExecutor

class SentimentAnalyzer:
    def __init__(self, model_name="SamLowe/roberta-base-go_emotions", max_threads=4):
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        self.sentiment_columns = ['joy', 'sadness', 'anger', 'fear', 'surprise', 'contentment', 'disgust', 'interest']
        self.max_threads = max_threads

    def analyze_sentiments(self, dataframe, text_column):
        sentiment_df = self.analyze_sentiments_for_dataframe(dataframe, text_column)
        result_dataframe = pd.concat([dataframe, sentiment_df], axis=1)
        return result_dataframe

    def analyze_sentiments_for_dataframe(self, dataframe, text_column):
        sentiment_scores = []
        text_list = dataframe[text_column].tolist()
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            sentiment_scores = list(executor.map(self.analyze_sentiment, text_list))
        sentiment_df = pd.DataFrame(sentiment_scores, columns=dataframe.columns.tolist().extend(self.sentiment_columns))
        return sentiment_df

    def analyze_sentiment(self, text):
        truncated_text = text[:512]
        inputs = self.tokenizer(truncated_text, return_tensors="pt", padding=True, truncation=True).to(self.device)
        with torch.no_grad():
            outputs = self.model(**inputs)
        probabilities = torch.softmax(outputs.logits, dim=1)[0]
        return [score.item() for score in probabilities]