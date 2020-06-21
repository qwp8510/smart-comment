# coding=utf-8
import numpy as np 
import pandas as pd 
import tensorflow as tf
from tensorflow import keras
import json
import os
from os.path import join, abspath, dirname

import collections
from .bert_tensorflow.modeling import BertModel
from ..config import Config


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
    
    # startLogits, endLogits = tf.split(logits, axis=-1, num_or_size_splits= 2, name='split')
    # startLogits = tf.squeeze(startLogits, axis=-1, name='start_squeeze')
    # endLogits = tf.squeeze(endLogits, axis=-1, name='end_squeeze')
    # ans_type = TDense(5, name='ans_type')(pooled_output)
    # return keras.Model([inp for inp in [unique_id, input_ids, input_masks, segment_ids]
    #                         if inp is not None],
    #                             [unique_id, startLogits, endLogits, ans_type],
    #                             name='bert-baseline')

def main():
    config = Config(join(CURRENT_PATH, 'bert_tensorflow/bert_config.json')).content
    # TDense(10).build(None)
    model= Model(config)
    print(model.summary())

if __name__ == '__main__':
    config = Config(join(CURRENT_PATH, 'bert_tensorflow/bert_config.json')).content
    # TDense(10).build(None)
    model= Model(config)
    model.summary()