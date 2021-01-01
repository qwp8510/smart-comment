import numpy as np
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from tensorflow import keras
import logging
from os import path
from datetime import datetime
from .model import Model
from eyescomment import get_json_content


CURRENT_DIR = path.dirname(path.abspath(__file__))
logger = logging.getLogger(__name__)
MODEL_DIR = path.join(CURRENT_DIR, 'models')


class Trainer():
    def __init__(self, model, batch_size, lossFunction='binary_crossentropy', learningRate=1e-4):
        utcnow_str = datetime.utcnow().replace(microsecond=0).isoformat()
        utcnow_str = utcnow_str.replace(':', '-')
        self.time_model_dir = path.join(MODEL_DIR, utcnow_str)
        os.makedirs(self.time_model_dir, exist_ok=True)
        self.model = model
        self.lossFunction = lossFunction
        self.lr = learningRate
        self.batch_size = batch_size

    def custom_loss(self, y_true, y_pred):
        return keras.callbacks.LearningRateScheduler(
            keras.backend.reshape(y_true[:, 0], (-1, 1)), y_pred) * y_true[:, 1]

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
        # tf.saved_model.save(self.model, model_idx_dir)
        self.model.save_weights('weights.ckpt')


def train(*args):
    trainging_data = args[0]
    h_train, unzip_x_train = trainging_data
    input_ids, input_segments, input_masks, y_train = zip(*unzip_x_train())
    input_ids, input_segments, input_masks, y_train = \
        np.array(input_ids), np.array(input_segments), np.array(input_masks), np.array(y_train)
    logger.info('training data shape ids:{}, segments:{}, masks:{}, y_train:{}'.format(
        input_ids.shape, input_segments.shape, input_masks.shape, y_train.shape))
    BertModel = Model(get_json_content(path.join(CURRENT_DIR, 'bert_tensorflow/bert_config.json')))
    x_train = [input_ids, input_masks, input_segments]
    batch_size = 32
    epoch = 1
    Trainer(BertModel, batch_size).fit(x_train, y_train, epochs=epoch)
