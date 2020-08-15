import torch
import torch.nn as nn
import random


class Encoder(nn.Module):
    def __init__(self, input_dim, emb_dim, hid_dim, n_layers, dropout):
        super().__init__()
        self.hid_dim = hid_dim
        self.n_layers = n_layers
        self.embedding = nn.Embedding(input_dim, emb_dim)
        self.lstm = nn.LSTM(emb_dim, hid_dim, n_layers, dropout=dropout, batch_first=True)
        self.dropout = nn.Dropout(dropout)

    def forward(self, src):
        #src = [batch size, src len]
        embedded = self.dropout(self.embedding(src))
        #embedded = [batch size, src len, embdim]
        outputs, (hidden, cell) = self.lstm(embedded)
        #outputs = [batch size, src len, hid dim * n directions]
        #hidden = [n layers * n directions, batch size, hid dim]
        #cell = [n layers * n directions, batch size, hid dim]

        #outputs are always from the top hidden layer
        return outputs, hidden, cell


class Decoder(nn.Module):
    def __init__(self, output_dim, emb_dim, hid_dim, n_layers, dropout):
        super().__init__()
        self.output_dim = output_dim
        self.hid_dim = hid_dim
        self.n_layers = n_layers
        self.embedding = nn.Embedding(output_dim, emb_dim)
        self.rnn = nn.LSTM(emb_dim, hid_dim, n_layers, dropout=dropout)
        self.fc_out = nn.Linear(hid_dim, output_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, inp, hidden, cell):
        #inp = [batch size]
        #hidden = [n layers * n directions, batch size, hid dim]
        #cell = [n layers * n directions, batch size, hid dim]
        inp = inp.unsqueeze(0)
        #inp = [1, batch size]
        embedded = self.dropout(self.embedding(inp))
        #embedded = [1, batch size, emb dim]
        output, (hidden, cell) = self.rnn(embedded, (hidden, cell))
        #output = [seq len, batch size, hid dim * n directions]
        #hidden = [n layers * n directions, batch size, hid dim]
        #cell = [n layers * n directions, batch size, hid dim]
        
        prediction = self.fc_out(output.squeeze(0))
        #prediction = [batch size, output dim]
        return prediction, hidden, cell


class Seq2Seq(nn.Module):
    def __init__(self, encoder, decoder=None, device=None):
        super().__init__()
        self.encoder = encoder
        self.decoder = decoder
        self.device = device

        assert encoder.hid_dim == decoder.hid_dim, \
            "Hidden dimensions of encoder and decoder must be equal!"
        assert encoder.n_layers == decoder.n_layers, \
            "Encoder and decoder must have equal number of layers!"

    def forward(self, src, trg, teacher_forcing_ratio = 0.5):
        #teacher_forcing_ratio is probability to use teacher forcing
        trg_vocab_size = self.decoder.output_dim
        #tensor to store decoder outputs
        target_length = trg.size(1)
        batch_size = trg.size(0)
        outputs = torch.zeros(target_length, batch_size, trg_vocab_size)

        #last hidden state of the encoder is used as the initial hidden state of the decoder
        result, hidden, cell = self.encoder(src)
        
        if not self.decoder:    return result 
        #first input to the decoder is the [CLS] tokens
        inp = trg[:, 0]   
        for t in range(target_length):
            output, hidden, cell = self.decoder(inp, hidden, cell)

            outputs[t] = output

            #decide if we are going to use teacher forcing or not
            teacher_force = random.random() < teacher_forcing_ratio
            
            #get the highest predicted token from our predictions
            top1 = output.argmax(1)
            inp = trg[:, t] if teacher_force else top1
        return outputs
