# coding=utf-8
import tensorflow as tf
from tensorflow import keras
from os.path import join, abspath, dirname
import torch
import torch.nn as nn
import random
from .bert_tensorflow.modeling import BertModel
from ...config import Config


CURRENT_PATH = dirname(abspath(__file__))


class TDense(keras.layers.Layer):
    def __init__(self,
                 output_size,
                 kernel_initializer=None,
                 bias_initializer="zeros",
                 **kwargs
    ):
        super().__init__(**kwargs)
        self.output_size = output_size
        self.kernel_initializer = kernel_initializer
        self.bias_initializer = bias_initializer

    def build(self, input_shape):
        dtype = tf.as_dtype(self.dtype or keras.backend.floatx())
        if not (dtype.is_floating or dtype.is_complex):
            raise TypeError("Unable to build `TDense` layer with non-floating point (and non-complex) dtype %s"% (dtype))
        input_shape = tf.TensorShape(input_shape)
        last_dim = tf.compat.dimension_value(input_shape[-1])
        if last_dim is None:
            raise ValueError("The last dimension of the inputs to `TDense` should be defined Found `None`.")
        self.input_spec = keras.layers.InputSpec(min_ndim=3, axes={-1: last_dim})
        self.kernel = self.add_weight(
            "kernel",
            shape=[self.output_size, last_dim],
            initializer=self.kernel_initializer,
            dtype=self.dtype,
            trainable=True
        )
        self.bias = self.add_weight(
            "bias",
            shape=[self.output_size],
            initializer=self.bias_initializer,
            dtype=self.dtype,
            trainable=True
        )
        super(TDense, self).build(input_shape)

    def call(self, x):
        return tf.matmul(x, self.kernel, transpose_b=True) + self.bias

def Model(config):
    output_size = 1
    seq_len = config['max_position_embeddings']
    unique_id = keras.Input(shape=(1,), dtype=tf.int64, name='unique_id')
    input_ids = keras.Input(shape=(seq_len,), dtype=tf.int32, name='input_ids')
    input_masks = keras.Input(shape=(seq_len,), dtype=tf.int32, name='input_mask')
    segment_ids = keras.Input(shape=(seq_len,), dtype=tf.int32, name='segment_ids')
    BERT = BertModel(config=config, name='bert')
    pooled_output, sequence_output = BERT(input_word_ids=input_ids,
                                          input_mask=input_masks,
                                          input_type_ids=segment_ids)
    logits = keras.layers.GlobalAveragePooling1D()(sequence_output)
    logits = keras.layers.Dropout(0.2)(logits)
    out = keras.layers.Dense(output_size, activation="sigmoid", name="dense_output")(logits)
    return keras.Model(inputs=[input_ids, input_masks, segment_ids], outputs=out)


class Attention(nn.Module):
    def __init__(self, feature_dim, step_dim, bias=True, **kwargs):
        super(Attention, self).__init__(**kwargs)
        self.bias = bias
        self.feature_dim = feature_dim
        self.step_dim = step_dim
        self.features_dim = 0
        
        weight = torch.zeros(feature_dim, 1)
        nn.init.xavier_uniform_(weight)
        self.weight = nn.Parameter(weight)
        if bias:
            self.b = nn.Parameter(torch.zeros(step_dim))
        
    def forward(self, x, mask=None):
        feature_dim = self.feature_dim
        step_dim = self.step_dim

        eij = torch.mm(
            x.contiguous().view(-1, feature_dim), 
            self.weight
        ).view(-1, step_dim)
        
        if self.bias:
            eij = eij + self.b
            
        eij = torch.tanh(eij)
        a = torch.exp(eij)
        
        if mask is not None:
            a = a * mask

        a = a / torch.sum(a, 1, keepdim=True) + 1e-10
        weighted_input = x * torch.unsqueeze(a, -1)
        return torch.sum(weighted_input, 1)


class NeuralNet(nn.Module):
    def __init__(
        self, input_dim, emb_dim, hidden_dim_1, hidden_dim_2, output_size, maxlen):
        super(NeuralNet, self).__init__()

        self.embedding = nn.Embedding(input_dim, emb_dim)
        
        self.lstm = nn.GRU(
            emb_dim, hidden_dim_1, bidirectional=True, batch_first=True)
        self.gru = nn.GRU(
            hidden_dim_1 * 2, hidden_dim_1, bidirectional=True, batch_first=True)
        
        self.lstm_attention = Attention(hidden_dim_1 * 2, maxlen)
        self.gru_attention = Attention(hidden_dim_1 * 2, maxlen)
        
        concate_dim = hidden_dim_1 * 2 * 4
        self.linear = nn.Linear(concate_dim, hidden_dim_2)
        self.relu = nn.ReLU()
        self.sigmoid = nn.Sigmoid()
        self.dropout = nn.Dropout(0.1)
        self.out = nn.Linear(hidden_dim_2, output_size)
        
    def forward(self, x):
        embedded = self.dropout(self.embedding(x))
        
        h_lstm, _ = self.lstm(embedded)
        h_gru, _ = self.gru(h_lstm)
        h_lstm_atten = self.lstm_attention(h_lstm)
        h_gru_atten = self.gru_attention(h_gru)
        avg_pool = torch.mean(h_gru, 1)
        max_pool, _ = torch.max(h_gru, 1)
        
        conc = torch.cat((h_lstm_atten, h_gru_atten, avg_pool, max_pool), 1)
        conc = self.relu(self.linear(conc))
        conc = self.dropout(conc)
        out = self.sigmoid(self.out(conc))
        return out


