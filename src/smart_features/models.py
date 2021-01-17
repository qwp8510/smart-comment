from .eyesComment import train as eyescomment_train
from .eyesComment.detector import SnowNlpSentimentDetector as eyescomment_detector
from .text_clustering import train as text_clustering_train


EYESCOMMENT_BEHAVIOR = {'train': eyescomment_train, 'predict': eyescomment_detector}
TEXT_CLUSTERING_BEHAVIOR = {'train': text_clustering_train}
MODEL_TYPE = {'eyesComment': EYESCOMMENT_BEHAVIOR, 'text_clustering': TEXT_CLUSTERING_BEHAVIOR}


class SmartFeatures():
    @staticmethod
    def model(model_type, model_behavior):
        """ SmartFeatures model
        Args:
            model_type(str): choose model(eyesComment, text_clustering...)
            model_behavior(str): choose model behavior(train, predict...)

        Returns:
            Model Instance

        """
        return MODEL_TYPE.get(model_type, {}).get(model_behavior)
