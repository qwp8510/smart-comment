import torch
import torch.nn as nn
import logging
from os import path
import pandas as pd
from sklearn.model_selection import train_test_split

from ..preprocess import load_smart_eyes_data
from .model import NeuralNet
from ...youtube.channel_api import ChannelApi
from ...config import Config


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))


class Trainer():
    def __init__(self, batch_size, model, optimizer, epochs):
        self.batch_size = batch_size
        self.epochs = epochs
        self.model = model
        self.optimizer = optimizer
        self.loss_fn = nn.L1Loss(reduce=None)

    def train_epoch(self, train_loader):
        self.model.train()
        avg_loss = 0.
        for x_batch, y_batch in train_loader:
            y_pred = self.model(x_batch)
            loss = self.loss_fn(y_pred, y_batch)
            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()
            avg_loss += loss.item() / len(train_loader)
            print(y_pred)
            print('loss:', loss.item())
        return avg_loss

    def train_val(self, val_loader):
        avg_loss = 0.
        for x_batch, y_batch in val_loader:
            y_pred = self.model(x_batch).detach()
            avg_loss += self.loss_fn(y_pred, y_batch).item() / len(val_loader)
        return avg_loss

    def fit(self, x_train, y_train, x_val, y_val):
        x_train_fold = torch.tensor(x_train, dtype=torch.long)
        y_train_fold = torch.tensor(y_train, dtype=torch.float32)
        x_val_fold = torch.tensor(x_val, dtype=torch.long)
        y_val_fold = torch.tensor(y_val, dtype=torch.float32)
        train_dataset = torch.utils.data.TensorDataset(x_train_fold, y_train_fold)
        valid_dataset = torch.utils.data.TensorDataset(x_val_fold, y_val_fold)
        train_loader = torch.utils.data.DataLoader(
            train_dataset, batch_size=self.batch_size, shuffle=True)
        valid_loader = torch.utils.data.DataLoader(
            valid_dataset, batch_size=self.batch_size, shuffle=False)
        for epoch in range(self.epochs):
            avg_train_loss = self.train_epoch(train_loader)
            avg_val_loss = self.train_val(valid_loader)
            logger.info('Epoch {} train loss: {}, val loss: {}'.format(
                epoch, avg_train_loss, avg_val_loss))


def main():
    max_features = 21128
    maxlen = 50
    BATCH_SIZE = 128
    INPUT_DIM = max_features
    EMB_DIM =128
    HID_DIM_1 = 60
    HID_DIM_2 = 16
    OUTPUT_DIM = 3
    h_data, text_data, y_data = load_smart_eyes_data()
    logger.info('text train shape: {}'.format(text_data.shape))
    x_train, x_val, y_train, y_val = train_test_split(text_data, y_data, test_size=0.2, random_state=42)
    model = NeuralNet(
        INPUT_DIM, EMB_DIM, HID_DIM_1, HID_DIM_2, OUTPUT_DIM, maxlen)
    optimizer = torch.optim.Adam(model.parameters())

    trainer = Trainer(
        batch_size=BATCH_SIZE,
        model=model,
        optimizer=optimizer,
        epochs=30)
    trainer.fit(x_train, y_train, x_val, y_val)
