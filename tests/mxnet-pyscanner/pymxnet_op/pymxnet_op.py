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
import time
import os.path
from urllib import urlretrieve
from timeit import default_timer as now
from io import BytesIO

import mxnet as mx
import numpy as np

from PIL import Image
from io import BytesIO
from collections import namedtuple

Batch = namedtuple('Batch', ['data'])

f_params = 'resnet-18-0000.params'
f_symbol = 'resnet-18-symbol.json'

#params
f_params_file = '/tmp/' + f_params

#symbol
f_symbol_file = '/tmp/' + f_symbol

#params
start = now()
if not os.path.isfile(f_params_file):
    print ("retrieving params")
    urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-0000.params", f_params_file)

#symbol
if not os.path.isfile(f_symbol_file):
    print ("retrieving symbols")
    urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-symbol.json", f_symbol_file)
end = now()
print('Time to download MXNet model: {:.4f} s'.format(end - start))

class PyMxnetKernel(scannerpy.Kernel):
  def __init__(self, config, protobufs):
    self.protobufs = protobufs

  def close(self):
    pass

  @staticmethod
  def load_model(s_fname, p_fname):
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

  @staticmethod
  def predict(batch_size, data, mod, synsets=None):
    # predict labels for a batch of images

    data_size = len(data)
    cnt = 0
    # center crop and resize
    # ** width, height must be greater than new_width, new_height 
    new_width, new_height = 224, 224
    labels = []
    while cnt < data_size:
      img_list = []
      for frame in data[cnt:cnt+batch_size]:
        img = frame
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
        img_list.append(img)
 
      # forward pass through the network
      start = now()
      batch = mx.io.DataBatch([mx.nd.array(img_list)], [])
      mod.forward(batch)
      probs = mod.get_outputs()[0].asnumpy()
      # print probs.shape
      stop = now()
      delta = stop - start
      print('Time to forward: {:.4f} s'.format(delta))

      for prob in probs:
        prob = np.squeeze(prob)
        a = np.argsort(prob)[::-1]
        labels.append(struct.pack('=i', a[0]))

      cnt += batch_size

    return labels

  @staticmethod
  def convertToJpeg(im):
    with BytesIO() as f:
      im.save(f, format='JPEG')

      return f.getvalue()

  def execute(self, input_columns):
    input_count = len(input_columns[0])
    column_count = len(input_columns)
    assert column_count == 1

    out_cols = []
    data = []
    for i in xrange(input_count):
        pil_im = Image.fromarray(input_columns[0][i])
        jpeg_image = self.convertToJpeg(pil_im) # also convert to jpeg
        img = Image.open(BytesIO(jpeg_image))
        data.append(img)
    print('batch = # {:d} images'.format(len(data)))
    start = now()
    sym, arg_params, aux_params = self.load_model(f_symbol_file, f_params_file)

    mod = mx.mod.Module(symbol=sym, label_names=None)
    mod.bind(for_training=False, data_shapes=[('data', (input_count,3,224,224))], label_shapes=mod._label_shapes)
    mod.set_params(arg_params, aux_params, allow_missing=True)
    stop = now()
    delta = stop - start
    print('Time to load model: {:.4f}s'.format(delta))

    start = now()
    labels = self.predict(input_count, data, mod) # a list of labels
    stop = now()
    delta = stop - start
    print('Time to predict: {:.4f}s'.format(delta))
        # print label
        # return [struct.pack('=i', label)]
    out_cols.append(labels)
    return out_cols

KERNEL = PyMxnetKernel




