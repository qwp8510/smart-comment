import os
import sys

sys.path.insert(0, "{}/src/".format(os.getcwd()))
from sentiment.sentiment_score import SentimentScore


def test_get_sentiment_score():
    result = SentimentScore().get('test')
    assert type(result) == dict
    assert result.get('sentimentScore') is not None
    assert type(result.get('sentimentScore')) == float
