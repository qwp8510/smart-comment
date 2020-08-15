import tensorflow as tf
from tensorflow import keras
from .bert_tensorflow.tokenization import FullTokenizer
from .bert_tensorflow.modeling import BertModel
from .preprocess import BertTokenInput
from .model import Model
from os import path
import numpy as np
import logging
import re
import json


CURRENT_DIR = path.dirname(path.abspath(__file__))
VOCAB_DIR = path.join(CURRENT_DIR, 'bert_tensorflow/assets/vocab.txt')


class TextTokener(FullTokenizer):
    def __init__(self, maxLength=30):
        super().__init__(VOCAB_DIR)
        self.maxLength = maxLength

    def _get_ids(self, token):
        PAD = 0
        if len(token) >= self.maxLength:
            return self.convert_tokens_to_ids(token)[:self.maxLength]
        else:
            return self.convert_tokens_to_ids(token) + [PAD] * (self.maxLength - len(token))

    def to_lowercase(self, text):
        # when tokenize chinese with english text, you must transform to lowercase for tokenize
        return text.lower()

    def clean_whitespace(self, token):
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        return _RE_COMBINE_WHITESPACE.sub(" ", token).strip()

    def tokenize_word(self, text):
        text = self.to_lowercase(text)
        clean_text = self.clean_whitespace(text)
        bert_text = '[CLS]' + ''.join(['[SEP]' if word == ' ' else word for word in clean_text])
        text_token = self.tokenize_chinese(bert_text)
        return self._get_ids(text_token)


class Predictor():
    MODEL_PATH = path.join(CURRENT_DIR, 'models', 'ver1', '6.h5')
    WEIGHTS_PATH = path.join(CURRENT_DIR, 'models', '2020-06-26T16-17-42', 'weights.ckpt')

    def __init__(self):
        MAX_SEQUENCE_LENGTH = 30
        # input_ids = tf.keras.layers.Input((MAX_SEQUENCE_LENGTH,), dtype=tf.int32)
        # input_mask = tf.keras.layers.Input((MAX_SEQUENCE_LENGTH,), dtype=tf.int32)
        # segment_ids = tf.keras.layers.Input((MAX_SEQUENCE_LENGTH,), dtype=tf.int32)
        # self.model = keras.models.load_model(self.MODEL_PATH, custom_objects={"BertModel": BertModel})
        with open('/Users/weichen/Desktop/smart_comment/src/eyesComment/bert_tensorflow/bert_config.json') as r:
            config_file = json.load(r)
            r.close()
        self.model = Model(config_file)
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

def main():
    textTokener = TextTokener()
    predictor = Predictor()
    texts = ['喜歡啾啾鞋的說書風格！不過中途不斷插入換台（章節）的音樂，個人感覺有點突兀，會中斷聽說書的情緒，有點可惜']
    unzip_x_train = BertTokenInput(texts, labels=[0], tokenizer=FullTokenizer(VOCAB_DIR))
    input_ids, input_segments, input_masks, y_train = zip(*unzip_x_train())
    input_ids, input_segments, input_masks, y_train = np.array(input_ids), np.array(input_segments), np.array(input_masks), np.array(y_train)
    # for text in texts:
    # text_token = textTokener.tokenize_word(text)
    result = predictor.predict([input_ids, input_masks, input_segments])
    logging.info('text: {} result score: {}'.format(texts, result))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()