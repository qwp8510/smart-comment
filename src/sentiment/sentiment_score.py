from smart_features.models import SmartFeatures


class SentimentScore():
    def __init__(self):
        self._model = None

    @property
    def model(self):
        if not self._model:
            self._model = SmartFeatures.model('eyesComment', 'predict')()
        return self._model

    def _predict_sentiment(self, text):
        return self.model.predict(text)

    def get(self, text):
        return {'sentimentScore': self._predict_sentiment(text)}
