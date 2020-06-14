import argparse
import logging
import os
from os import path
import numpy as np
import pandas as pd
from sklearn.utils import shuffle
import jieba.posseg as pseg
from tensorflow import keras
from .bert_tensorflow.tokenization import FullTokenizer

logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))
VOCAB_DIR = path.join(CURRENT_PATH, 'bert_tensorflow/assets/vocab.txt')

class HandleTextToInput():
    MAX_NUM_WORDS = 100000
    MAX_SEQ_LENGTH = 100

    def __init__(self):
        self.Tokenizer = self.tokenizer

    def jieba_tokenize(self, text):
        words = pseg.cut(text)
        return ' '.join([word for word, flag in words if flag != 'x'])

    @property
    def tokenizer(self):
        return keras.preprocessing.text.Tokenizer(num_words=self.MAX_NUM_WORDS)

    def gen_Textsequences(self, *texts):
        for text in texts:
            yield self.Tokenizer.texts_to_sequences(text)

    def padding(self, *texts):
        for text in self.gen_Textsequences(*texts):
            yield from keras.preprocessing.sequence.pad_sequences(text, maxlen=self.MAX_SEQ_LENGTH)

    def fit(self, x_text):
        x_text = x_text.apply(self.jieba_tokenize)
        corpus = x_text
        self.Tokenizer.fit_on_texts(corpus)
        train_text = list(self.padding(x_text))
        return train_text

class BertTokenInput():
    def __init__(self, texts, labels, tokenizer, maxLength=30):
        self.tokenizer = tokenizer
        self.texts = texts
        self.labels = labels
        self.maxLength = maxLength

    def _get_ids(self, token):
        wordToken = self.tokenizer.tokenize(token)
        if len(wordToken) >= self.maxLength:
            return self.tokenizer.convert_tokens_to_ids(wordToken)[:self.maxLength]
        else:
            return self.tokenizer.convert_tokens_to_ids(wordToken) + [0] * (self.maxLength - len(wordToken))

    def _get_segments(self, token):
        def one_hot_encoding():
            for idx, word in enumerate(token.split('[SEP]', 1)):
                yield from [idx] * len(word)
        segments = list(one_hot_encoding())
        if len(segments) >= self.maxLength:
            return segments[:self.maxLength]
        else:
            return segments + [0] * (self.maxLength - len(segments))

    def _get_masks(self, token):
        if len(token) >= self.maxLength:
            return [1] * self.maxLength
        else:
            return [1] * len(token) + [0] * (self.maxLength - len(token))

    def __call__(self):
        for _, text in enumerate(self.texts):
            token = '[CLS]' + ''.join(['[SEP]' if word == ' ' else word for word in text])
            input_ids = self._get_ids(token)
            input_segments = self._get_segments(token)
            input_masks = self._get_masks(token)
            yield np.asarray(input_ids, dtype=np.int32), np.asarray(input_segments, dtype=np.int32), np.asarray(input_masks, dtype=np.int32)

def trans_dfToData(df):
    head_cols = df.columns.tolist()[:2]
    label_cols = df.columns.tolist()[6:9]
    for _, row in df.iterrows():
        h_data = [row[col] for col in head_cols]
        y_data = [row[col] for col in label_cols]
        yield h_data, y_data

def load_file(files_dir):
    files = [os.path.join(files_dir, f) for f in os.listdir(files_dir) if f.endswith('.csv')]
    df = pd.concat([pd.read_csv(f, encoding='utf-8') for f in files], ignore_index=True)
    # df['text'] = pd.Series(HandleTextToInput().fit(df.loc[:, 'text']))
    shuffled_df = shuffle(df).reset_index(drop=True)
    unzip_train_data = BertTokenInput(shuffled_df['text'], shuffled_df['toxic'], FullTokenizer(VOCAB_DIR))
    head_data, label_data = zip(*trans_dfToData(shuffled_df))
    return head_data, unzip_train_data, np.array(label_data)

def load_trainingData():
    train_dir = path.join(CURRENT_PATH, 'data/training')
    return load_file(train_dir)

def main():
    h_train, unzip_x_train, y_train = load_trainingData()
    input_ids, input_segments, input_masks = zip(*unzip_x_train())
    input_ids, input_segments, input_masks = np.array(input_ids), np.array(input_segments), np.array(input_masks)
    logger.info('ids shape: {}, segments shape {}, masks shape {}'.format(input_ids.shape, input_segments.shape, input_masks.shape))
    logger.info(input_ids)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()