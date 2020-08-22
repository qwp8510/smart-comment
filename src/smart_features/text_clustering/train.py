import torch
import torch.nn as nn
import logging
from os import path
from sklearn.model_selection import train_test_split

from model import Encoder, Decoder, Seq2Seq


logger = logging.getLogger(__name__)
CURRENT_PATH = path.dirname(path.abspath(__file__))


class Trainer():
    def __init__(self, batch_size, model, optimizer, epochs):
        self.batch_size = batch_size
        self.epochs = epochs
        self.model = model
        self.optimizer = optimizer
        self.loss_fn = nn.CrossEntropyLoss()

    def train_epoch(self, train_loader):
        self.model.train()
        avg_loss = 0.
        for x_batch, y_batch in train_loader:
            y_pred = self.model(x_batch, y_batch)
            pred_dim = y_pred.shape[-1]
            output = y_pred.view(-1, pred_dim)
            y_batch = y_batch.view(-1)
            loss = self.loss_fn(output, y_batch)
            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()
            avg_loss += loss.item() / len(train_loader)
        return avg_loss

    def train_val(self, val_loader):
        avg_loss = 0.
        for x_batch, y_batch in val_loader:
            y_pred = self.model(x_batch, y_batch).detach()
            pred_dim = y_pred.shape[-1]
            output = y_pred.view(-1, pred_dim)
            y_batch = y_batch.view(-1)
            avg_loss += self.loss_fn(output, y_batch).item() / len(val_loader)
        return avg_loss

    def fit(self, x_train, y_train, x_val, y_val):
        x_train_fold = torch.tensor(x_train, dtype=torch.long)
        y_train_fold = torch.tensor(y_train, dtype=torch.long)
        x_val_fold = torch.tensor(x_val, dtype=torch.long)
        y_val_fold = torch.tensor(y_val, dtype=torch.long)
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


def train(*args):
    max_features = 21128
    BATCH_SIZE = 128
    INPUT_DIM = max_features
    OUTPUT_DIM = max_features
    ENC_EMB_DIM = 256
    DEC_EMB_DIM = 256
    HID_DIM = 512
    N_LAYERS = 2
    ENC_DROPOUT = 0.5
    DEC_DROPOUT = 0.5
    training_data = args[0]
    h_train, text_train = training_data
    logger.info('text train shape: {}'.format(text_train.shape))
    h_train, h_val, t_train, t_val = train_test_split(
        h_train, text_train, test_size=0.2, random_state=42)
    enc = Encoder(INPUT_DIM, ENC_EMB_DIM, HID_DIM, N_LAYERS, ENC_DROPOUT)
    dec = Decoder(OUTPUT_DIM, DEC_EMB_DIM, HID_DIM, N_LAYERS, DEC_DROPOUT)
    model = Seq2Seq(enc, dec)
    optimizer = torch.optim.Adam(model.parameters())

    trainer = Trainer(
        batch_size=BATCH_SIZE,
        model=model,
        optimizer=optimizer,
        epochs=30)
    trainer.fit(t_train, t_train, t_val, t_val)
