# from ..tokenization import FullTokenizer
from .model import Model
from eyescomment.config import Config
from os import path
import numpy as np
import logging
import re


CURRENT_DIR = path.dirname(path.abspath(__file__))
VOCAB_DIR = path.join(CURRENT_DIR, 'bert_tensorflow/assets/vocab.txt')


# class TextTokener(FullTokenizer):
#     def __init__(self, maxLength=30):
#         super().__init__(VOCAB_DIR)
#         self.maxLength = maxLength

#     def _get_ids(self, token):
#         PAD = 0
#         if len(token) >= self.maxLength:
#             return self.convert_tokens_to_ids(token)[:self.maxLength]
#         else:
#             return self.convert_tokens_to_ids(token) + [PAD] * (self.maxLength - len(token))

#     def to_lowercase(self, text):
#         # when tokenize chinese with english text, you must transform to lowercase for tokenize
#         return text.lower()

#     def clean_whitespace(self, token):
#         _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
#         return _RE_COMBINE_WHITESPACE.sub(" ", token).strip()

#     def tokenize_word(self, text):
#         text = self.to_lowercase(text)
#         clean_text = self.clean_whitespace(text)
#         bert_text = '[CLS]' + ''.join(['[SEP]' if word == ' ' else word for word in clean_text])
#         text_token = self.tokenize_chinese(bert_text)
#         return self._get_ids(text_token)


class Predictor():
    MODEL_PATH = path.join(CURRENT_DIR, 'models', 'ver1', '6.h5')
    WEIGHTS_PATH = path.join(CURRENT_DIR, 'models', '2020-06-26T16-17-42', 'weights.ckpt')

    def __init__(self):
        # MAX_SEQUENCE_LENGTH = 30
        # input_ids = tf.keras.layers.Input((MAX_SEQUENCE_LENGTH,), dtype=tf.int32)
        # input_mask = tf.keras.layers.Input((MAX_SEQUENCE_LENGTH,), dtype=tf.int32)
        # segment_ids = tf.keras.layers.Input((MAX_SEQUENCE_LENGTH,), dtype=tf.int32)
        # self.model = keras.models.load_model(
        # self.MODEL_PATH, custom_objects={"BertModel": BertModel})
        config = Config(path.join(CURRENT_DIR, 'bert_tensorflow/bert_config.json')).read()
        self.model = Model(config)
        self.model.load_weights(self.WEIGHTS_PATH)
        # infer = model.signatures["serving_default"]
        # models = infer('input_ids', 'input_mask', 'segment_ids')

        # models = infer(input_ids=q_id, input_mask=q_mask, segment_ids=q_atn)
        # self.models = self.model(input_word_ids=q_id, input_mask=q_mask, input_type_ids=q_atn)
        self.models = self.model

        print(self.models.summary())

    def predict(self, text_token):
        result = self.models.predict(text_token)
        return result


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()
