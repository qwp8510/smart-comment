import numpy as np 
import pandas as pd 
import tensorflow as tf
from tensorflow import keras
import json
import logging
import os
from os import path

import collections
from .model import Model
from ..config import Config
from .preprocess import load_trainingData
from sklearn.model_selection import train_test_split

CURRENT_PATH = path.dirname(path.abspath(__file__))
logger = logging.getLogger(__name__)

class Train():
    def __init__(self, model, batchSize, lossFunction='binary_crossentropy', learningRate=1e-4):
        self.model = model
        self.lossFunction = lossFunction
        self.lr = learningRate
        self.batchSize = batchSize

    def custom_loss(self, y_true, y_pred):
        return keras.callbacks.LearningRateScheduler(keras.backend.reshape(y_true[:,0],(-1,1)), y_pred) * y_true[:,1]

    @property
    def optimizer(self):
        return keras.optimizers.Adam(learning_rate=self.lr)

    def train_epoch(self, x_train, y_train, epochIdx):
        self.model.fit(
            x_train,
            y_train,
            batch_size=self.batchSize,
            epochs=1,
            verbose=1,
            validation_split=0.1,
            callbacks=[
                keras.callbacks.LearningRateScheduler(lambda epoch: 1e-3 * (0.6 ** epochIdx))
            ]
        )

    def fit(self, x_train, y_train, x_test=None, y_test=None, epochs=5):
        self.model.compile(loss=self.lossFunction, optimizer=self.optimizer)
        for epochIdx in range(epochs):
            self.train_epoch(x_train, y_train, epochIdx)

def main():
    h_train, unzip_x_train, y_train = load_trainingData()
    input_ids, input_segments, input_masks = zip(*unzip_x_train())
    input_ids, input_segments, input_masks = np.array(input_ids), np.array(input_segments), np.array(input_masks)
    logger.info('training data shape ids:{}, segments:{}, masks:{}'.format(input_ids.shape, input_segments.shape, input_masks.shape))
    BertModel = Model(Config(path.join(CURRENT_PATH, 'bert_tensorflow/bert_config.json')).content)
    x_train = [input_ids, input_masks, input_segments]
    batchsize = 32
    Train(BertModel, batchsize).fit(x_train, y_train)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)-15s:%(levelname)s:%(name)s:%(message)s',
    )
    main()