import numpy as np 
import pandas as pd 
import tensorflow as tf
from tensorflow import keras
import json
import logging
import os
from os import path
from datetime import datetime
import collections
from .model import Model
from ..config import Config
from .preprocess import load_trainingData
from sklearn.model_selection import train_test_split

CURRENT_DIR = path.dirname(path.abspath(__file__))
logger = logging.getLogger(__name__)
MODEL_DIR = os.path.join(CURRENT_DIR, 'models')
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


class Train():
    def __init__(self, model, batch_size, lossFunction='binary_crossentropy', learningRate=1e-4):
        utcnow_str = datetime.utcnow().replace(microsecond=0).isoformat()
        self.time_model_dir = os.path.join(MODEL_DIR, utcnow_str)
        os.makedirs(self.time_model_dir, exist_ok=True)
        self.model = model
        self.lossFunction = lossFunction
        self.lr = learningRate
        self.batch_size = batch_size

    def custom_loss(self, y_true, y_pred):
        return keras.callbacks.LearningRateScheduler(keras.backend.reshape(y_true[:,0],(-1,1)), y_pred) * y_true[:,1]

    @property
    def optimizer(self):
        return keras.optimizers.Adam(learning_rate=self.lr)

    def train_epoch(self, x_train, y_train, epoch_idx):
        self.model.fit(
            x_train,
            y_train,
            batch_size=self.batch_size,
            epochs=1,
            verbose=1,
            validation_split=0.1,
            callbacks=[
                keras.callbacks.LearningRateScheduler(lambda epoch: 1e-3 * (0.6 ** epoch_idx))
            ]
        )

    def fit(self, x_train, y_train, x_test=None, y_test=None, epochs=5):
        self.model.compile(loss=self.lossFunction, optimizer=self.optimizer)
        for epoch_idx in range(epochs):
            self.train_epoch(x_train, y_train, epoch_idx)
            self.save(epoch_idx)

    def save(self, idx):
        model_idx_dir = path.join(self.time_model_dir, "{}.h5".format(idx))
        self.model.save(model_idx_dir)


def main():
    h_train, unzip_x_train, y_train = load_trainingData()
    input_ids, input_segments, input_masks = zip(*unzip_x_train())
    input_ids, input_segments, input_masks = np.array(input_ids), np.array(input_segments), np.array(input_masks)
    logger.info('training data shape ids:{}, segments:{}, masks:{}'.format(input_ids.shape, input_segments.shape, input_masks.shape))
    BertModel = Model(Config(path.join(CURRENT_DIR, 'bert_tensorflow/bert_config.json')).content)
    x_train = [input_ids, input_masks, input_segments]
    batch_size = 32
    Train(BertModel, batch_size).fit(x_train, y_train)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()