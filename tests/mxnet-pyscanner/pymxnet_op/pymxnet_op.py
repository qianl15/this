######################################################
# -*- coding: utf-8 -*-
# File Name: pymxnet_op.py
# Author: Qian Li
# Created Date: 2017-11-07
# Description: The kernel for MXNet python kernel
######################################################
import scannerpy
import struct
import numpy as np
import StringIO
import time
import sys
import cv2
import os
import os.path
import json
import tempfile
import urllib2 
from urllib import urlretrieve
from timeit import default_timer as now

import mxnet as mx
import numpy as np

from PIL import Image
from io import BytesIO
import base64
from collections import namedtuple

Batch = namedtuple('Batch', ['data'])

f_params = 'resnet-18-0000.params'
f_symbol = 'resnet-18-symbol.json'

#params
f_params_file = '/tmp/' + f_params
if not os.path.isfile(f_params_file):
    urlretrieve("http://data.dmlc.ml/mxnet/models/imagenet/resnet/18-layers/resnet-18-0000.params", f_params_file)

#symbol
f_symbol_file = '/tmp/' + f_symbol
if not os.path.isfile(f_symbol_file):
    urlretrieve("http://data.dmlc.ml/mxnet/models/imagenet/resnet/18-layers/resnet-18-symbol.json", f_symbol_file)

class PyMxnetKernel(scannerpy.Kernel):
  def __init__(self, config, protobufs):
    self.protobufs = protobufs

  def close(self):
    pass

  def load_model(self, s_fname, p_fname):
    # Load model checkpoint from file.
    # :return: (arg_params, aux_params)
    # arg_params : dict of str to NDArray
    #   Model parameter, dict of name to NDArray of net's weights.
    # aux_params : dict of str to NDArray
    #   Model parameter, dict of name to NDArray of net's auxiliary states.
    symbol = mx.symbol.load(s_fname)
    save_dict = mx.nd.load(p_fname)
    arg_params = {}
    aux_params = {}
    for k, v in save_dict.items():
      tp, name = k.split(':', 1)
      if tp == 'arg':
        arg_params[name] = v
      if tp == 'aux':
        aux_params[name] = v
    return symbol, arg_params, aux_params

  def predict(self, img, mod, synsets=None):
    # predict labels for a given image


    # PIL conversion
    #size = 224, 224
    #img = img.resize((224, 224), Image.ANTIALIAS)

    # center crop and resize
    # ** width, height must be greater than new_width, new_height 
    new_width, new_height = 224, 224
    width, height = img.size   # Get dimensions
    left = (width - new_width)/2
    top = (height - new_height)/2
    right = (width + new_width)/2
    bottom = (height + new_height)/2

    img = img.crop((left, top, right, bottom))
    # convert to numpy.ndarray
    sample = np.asarray(img)
    # swap axes to make image from (224, 224, 3) to (3, 224, 224)
    sample = np.swapaxes(sample, 0, 2)
    img = np.swapaxes(sample, 1, 2)
    img = img[np.newaxis, :] 
   
    # forward pass through the network
    mod.forward(Batch([mx.nd.array(img)]))
    prob = mod.get_outputs()[0].asnumpy()
    prob = np.squeeze(prob)
    a = np.argsort(prob)[::-1]
    
    out = a[0]

    return out

  def execute(self, input_columns):
    # cv2_im = cv2.cvtColor(input_columns[0],cv2.COLOR_BGR2RGB)
    pil_im = Image.fromarray(input_columns[0])
    # width, height = pil_im.size
    # print('width {}, height {}'.format(width, height))
    start = now()
    sym, arg_params, aux_params = self.load_model(f_symbol_file, f_params_file)

    mod = mx.mod.Module(symbol=sym, label_names=None)
    mod.bind(for_training=False, data_shapes=[('data', (1,3,224,224))], label_shapes=mod._label_shapes)
    mod.set_params(arg_params, aux_params, allow_missing=True)
    stop = now()
    delta = stop - start
    print('Time to load model: {:.4f}s'.format(delta))

    start = now()
    label = self.predict(pil_im, mod)
    stop = now()
    delta = stop - start
    print('Time to predict: {:.4f}s'.format(delta))
    # print label
    return [struct.pack('=i', label)]

KERNEL = PyMxnetKernel




