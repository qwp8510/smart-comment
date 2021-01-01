import logging
import os
import re
from os import path
import numpy as np
import pandas as pd
from sklearn.utils import shuffle
import jieba.posseg as pseg
from tensorflow import keras
from .tokenization import FullTokenizer
from eyescomment.config import Config
from eyescomment.md import Mongodb
from eyescomment.youtube import YoutubeChannel


logger = logging.getLogger(__name__)
CURRENT_DIR = path.dirname(path.abspath(__file__))
VOCAB_DIR = path.join(CURRENT_DIR, 'assets/vocab.txt')


class MdCommentLoader(Mongodb):
    MD_TRAINING_DIR = path.join(CURRENT_DIR, 'data/md_training')
    columns = [
        'commentId', 'author', 'videoId', 'publishedAt', 'updatedAt',
        'replyCount', 'likeCount', 'text'
    ]

    def __init__(
        self,
        cluster='raw-comment-chinese',
        db='comment-chinese',
        collection=None
    ):
        self.cluster = cluster
        self.database = db
        self.collection = collection

    def gen_collection(self, channel_id):
        return 'comment-{}'.format(channel_id)

    def gen_comment_dataset(self):
        for detail in self.get():
            features_data = {
                'commentId': detail.get('commentId'),
                'author': detail.get('author'),
                'videoId': detail.get('videoId'),
                'publishedAt': detail.get('publishedAt'),
                'updatedAt': detail.get('updatedAt'),
                'replyCount': detail.get('replyCount'),
                'likeCount': detail.get('likeCount'),
                'text': re.sub(' ', '', detail.get('text'))
            }
            yield features_data

    def _save(self, dataframe, channel_id):
        logger.info('save channel_id: {} dataframe'.format(channel_id))
        file_name = path.join(
            self.MD_TRAINING_DIR, 'md_data_{}.csv'.format(channel_id))
        dataframe.to_csv(file_name, index=False, encoding='utf-8-sig')

    def gen(self):
        channels = YoutubeChannel(
            host=Config.instance().get('PORTAL_SERVER'),
            cache_path=Config.instance().get('CACHE_DIR'),
            filter_params={"fields": {"channelId": True}})
        for channel in channels:
            channel_id = channel['channelId']
            collection = self.gen_collection(channel_id)
            super(MdCommentLoader, self).__init__(
                cluster_name=self.cluster, db_name=self.database, collection_name=collection)
            logger.info('gen channel id: {} comments data'.format(channel_id))
            comments_dataset = list(self.gen_comment_dataset())
            comments_dataframe = pd.DataFrame(comments_dataset, columns=self.columns)
            self._save(comments_dataframe, channel_id)


class JiebaTextTokenizer():
    MAX_NUM_WORDS = 100000
    MAX_SEQ_LENGTH = 100

    def __init__(self, max_num_words=100000, max_seq_length=100):
        self.max_num_words = max_num_words
        self.max_seq_length = max_seq_length
        self.Tokenizer = self.tokenizer

    def jieba_tokenize(self, text):
        words = pseg.cut(text)
        return ' '.join([word for word, flag in words if flag != 'x'])

    @property
    def tokenizer(self):
        return keras.preprocessing.text.Tokenizer(num_words=self.max_num_words)

    def gen_Textsequences(self, *texts):
        for text in texts:
            yield self.Tokenizer.texts_to_sequences(text)

    def padding(self, *texts):
        for text in self.gen_Textsequences(*texts):
            yield from keras.preprocessing.sequence.pad_sequences(text, maxlen=self.max_seq_length)

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

    def clean_whitespace(self, token):
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        return _RE_COMBINE_WHITESPACE.sub(" ", token).strip()

    def to_lowercase(self, text):
        # when tokenize chinese with english text, you must transform to lowercase for tokenize
        return text.lower()

    def _get_ids(self, token):
        PAD = 0
        if len(token) >= self.maxLength:
            return self.tokenizer.convert_tokens_to_ids(token)[:self.maxLength]
        else:
            return self.tokenizer.convert_tokens_to_ids(token)\
                + [PAD] * (self.maxLength - len(token))

    def _get_segments(self, token):
        segment = 0

        def get_segment(segment):
            if segment == 0:
                return 1
            else:
                return 0

        def convert_to_one_hot(segment):
            for word in token:
                yield segment
                if word == '[SEP]':
                    segment = get_segment(segment)
        segments = list(convert_to_one_hot(segment))
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
        for idx, text in enumerate(self.texts):
            if isinstance(text, str):
                text = self.to_lowercase(text)
                clean_text = self.clean_whitespace(text)
                token = '[CLS]' + ''.join(['[SEP]' if word == ' ' else word for word in clean_text])
                wordToken = self.tokenizer.tokenize_chinese(token)
                input_ids = self._get_ids(wordToken)
                input_segments = self._get_segments(wordToken)
                input_masks = self._get_masks(wordToken)
                yield np.asarray(input_ids, dtype=np.int32),\
                    np.asarray(input_segments, dtype=np.int32),\
                    np.asarray(input_masks, dtype=np.int32),\
                    np.asarray(self.labels[idx], dtype=np.int32)


class BertTokenizer():
    def __init__(self, tokenizer, max_length=30, labels=None):
        self.tokenizer = tokenizer
        self.maxLength = max_length
        self.labels = labels

    def clean_whitespace(self, token):
        _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        return _RE_COMBINE_WHITESPACE.sub(" ", token).strip()

    def to_lowercase(self, text):
        # when tokenize chinese with english text, you must transform to lowercase for tokenize
        return text.lower()

    def get_id(self, token):
        PAD = 0
        if len(token) >= self.maxLength:
            return self.tokenizer.convert_tokens_to_ids(token)[:self.maxLength]
        else:
            return self.tokenizer.convert_tokens_to_ids(token)\
                + [PAD] * (self.maxLength - len(token))

    def fit(self, texts):
        for idx, text in enumerate(texts):
            if isinstance(text, str):
                text = self.to_lowercase(text)
                clean_text = self.clean_whitespace(text)
                word_token = self.tokenizer.tokenize_chinese(clean_text)
                yield np.array(self.get_id(word_token))


def trans_dfToData(df):
    head_cols = df.columns.tolist()[:2]
    # label_cols = df.columns.tolist()[6:9]
    label_cols = df.columns.tolist()[6:7]
    for _, row in df.iterrows():
        h_data = [row[col] for col in head_cols]
        y_data = [row[col] for col in label_cols]
        yield h_data, y_data


def convert_data(df):
    head_cols = df.columns.tolist()[:7]
    for _, row in df.iterrows():
        h_data = [row[col] for col in head_cols]
        yield h_data


def convert_smart_eyes_data(df):
    head_cols = df.columns.tolist()[:3]
    label_cols = df.columns.tolist()[6:9]
    for _, row in df.iterrows():
        h_data = [row[col] for col in head_cols]
        y_data = [row[col] for col in label_cols]
        yield h_data, np.array(y_data)


def _load_dataframe(file_dir):
    files = [path.join(file_dir, f) for f in os.listdir(file_dir) if f.endswith('.csv')]
    df = pd.concat(
        [pd.read_csv(open(f, 'rU'), encoding='utf-8', engine='c') for f in files],
        ignore_index=True)
    shuffled_df = shuffle(df).reset_index(drop=True)
    return shuffled_df


def load_comments():
    train_dir = path.join(CURRENT_DIR, 'text_clustering/data/training')
    df = _load_dataframe(train_dir)
    h_data = list(convert_data(df))
    logger.info('processing tokenize text')
    texts_feature = list(BertTokenizer(
        tokenizer=FullTokenizer(VOCAB_DIR), max_length=30).fit(df['text'].astype('str')))
    return np.array(h_data), np.array(texts_feature)


def load_smart_eyes_data():
    train_dir = path.join(CURRENT_DIR, 'data/training')
    df = _load_dataframe(train_dir)
    texts_feature = list(BertTokenizer(
        tokenizer=FullTokenizer(VOCAB_DIR), max_length=50).fit(df['text'].astype('str')))
    h_data, y_data = zip(*convert_smart_eyes_data(df))
    return h_data, np.array(texts_feature), np.array(y_data)


def load_file(files_dir):
    files = [os.path.join(files_dir, f) for f in os.listdir(files_dir) if f.endswith('.csv')]
    df = pd.concat([pd.read_csv(f, encoding='utf-8') for f in files], ignore_index=True)
    shuffled_df = shuffle(df).reset_index(drop=True)
    head_data, label_data = zip(*trans_dfToData(shuffled_df))
    unzip_dataset = BertTokenInput(
        shuffled_df['text'], shuffled_df['toxic'], FullTokenizer(VOCAB_DIR))
    return head_data, unzip_dataset


def load_train_data():
    train_dir = path.join(CURRENT_DIR, 'eyesComment/data/training')
    return load_file(train_dir)


def main():
    Config.set_dir(path.join(CURRENT_DIR, '../config.json'))
    MdCommentLoader(
        cluster='raw-comment-chinese', db='comment-chinese'
    ).gen()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()
