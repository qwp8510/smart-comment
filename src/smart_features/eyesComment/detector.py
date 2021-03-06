from os import path
import requests
import logging
from nltk.sentiment import SentimentIntensityAnalyzer
from translate import Translator
from snownlp import SnowNLP

from .model import Model
from eyescomment.config import Config
from eyescomment import get_json_content


CURRENT_DIR = path.dirname(path.abspath(__file__))
VOCAB_DIR = path.join(CURRENT_DIR, 'bert_tensorflow/assets/vocab.txt')
logger = logging.getLogger(__name__)


class Predictor():
    MODEL_PATH = path.join(CURRENT_DIR, 'models', 'ver1', '6.h5')
    WEIGHTS_PATH = path.join(CURRENT_DIR, 'models', '2020-06-26T16-17-42', 'weights.ckpt')

    def __init__(self):
        config = get_json_content(path.join(CURRENT_DIR, 'bert_tensorflow/bert_config.json'))
        self.model = Model(config)
        self.model.load_weights(self.WEIGHTS_PATH)
        self.models = self.model

    def predict(self, text_token):
        result = self.models.predict(text_token)
        return result


class SentimentDetector():
    def __init__(self):
        self.URL = Config.instance().get('SENTIMENT_API_URL')
        self._sess = None

    @property
    def sess(self):
        if not self._sess:
            self._sess = requests.Session()
        return self._sess

    def _enrich_data(self, text):
        return {
            'model': "dl",
            'output_level': "subject",
            'show_scores': True,
            'text': text
        }

    def predict(self, text):
        response = self.sess.post(self.URL, json=self._enrich_data(text))
        if response.status_code == 200:
            return (response.json().get('overall_res', {})).get('sentiment_score')


class NltkSentimentDetector():
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()
        self.translator = Translator(from_lang="chinese", to_lang="english")

    def predict(self, text):
        try:
            en_text = self.translator.translate(text)
            if en_text:
                score = self.sia.polarity_scores(en_text)
                return score.get('compound')
            else:
                return None
        except Exception as err:
            logger.error('GoogleSentimentDetector fail with {}'.format(err))


class SnowNlpSentimentDetector():
    def predict(self, text):
        return SnowNLP(text).sentiments
