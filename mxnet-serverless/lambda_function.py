'''
Reference code to showcase MXNet model prediction on AWS Lambda 

@author: Sunil Mallya (smallya@amazon.com)
version: 0.2
'''

import os
import boto3
import json
# import tempfile
import urllib2 
from urllib import urlretrieve

import mxnet as mx
import numpy as np

from PIL import Image
from io import BytesIO
import base64
from collections import namedtuple
import os.path
Batch = namedtuple('Batch', ['data'])

f_params = 'resnet-18-0000.params'
f_symbol = 'resnet-18-symbol.json'
    
#params

f_params_file = '/tmp/' + f_params
if not os.path.isfile(f_params_file):
    urlretrieve("http://data.dmlc.ml/mxnet/models/imagenet/resnet/18-layers/resnet-18-0000.params", f_params_file)
    # f_params_file.flush()

#symbol
f_symbol_file = '/tmp/' + f_symbol
if not os.path.isfile(f_symbol_file):
    urlretrieve("http://data.dmlc.ml/mxnet/models/imagenet/resnet/18-layers/resnet-18-symbol.json", f_symbol_file)
    # f_symbol_file.flush()

def load_model(s_fname, p_fname):
    """
    Load model checkpoint from file.
    :return: (arg_params, aux_params)
    arg_params : dict of str to NDArray
        Model parameter, dict of name to NDArray of net's weights.
    aux_params : dict of str to NDArray
        Model parameter, dict of name to NDArray of net's auxiliary states.
    """
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

def predict(b64Img, mod, synsets=None):
    '''
    predict labels for a given image
    '''

    #req = urllib2.urlopen(url)
    #img_file = tempfile.NamedTemporaryFile()
    #img_file.write(req.read())
    #img_file.flush()
 
    #img = Image.open(img_file.name)
    img = Image.open(BytesIO(base64.b64decode(b64Img)))

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
    
    # out = '{"0" : {"%s" : "%s"}' %(synsets[a[0]], prob[a[0]]) 
    # cnt = 0;
    # for i in a[1:5]:
    #     cnt += 1;
    #     out += ', "%d" : {"%s" : "%s"}' %(cnt, synsets[i], prob[i])
    # out += "}"

    # just return the index, not the synset!
    out = '{"0" : {"%s" : "%s"}' %(a[0], prob[a[0]]) 
    cnt = 0;
    for i in a[1:5]:
        cnt += 1;
        out += ', "%d" : {"%s" : "%s"}' %(cnt, i, prob[i])
    out += "}"

    return out

# with open('synset.txt', 'r') as f:
#     synsets = [l.rstrip() for l in f]

def lambda_handler(event, context):

    # print("Event is :")
    # print(event)
    b64Img = ''
    try:
        # API Gateway GET method
        if event['httpMethod'] == 'GET':
            url = event['queryStringParameters']['url']
        # API Gateway POST method
        elif event['httpMethod'] == 'POST':
            data = json.loads(event['body'])
            # print("POST method, its data is:")
            # print(data['b64Img'])
            b64Img = data['b64Img']
    except KeyError:
        # direct invocation
        b64Img = event['b64Img']
    
    sym, arg_params, aux_params = load_model(f_symbol_file, f_params_file)
    mod = mx.mod.Module(symbol=sym, label_names=None)
    mod.bind(for_training=False, data_shapes=[('data', (1,3,224,224))], label_shapes=mod._label_shapes)
    mod.set_params(arg_params, aux_params, allow_missing=True)
    # labels = predict(b64Img, mod, synsets)
    labels = predict(b64Img, mod)

    out = {
            "headers": {
                "content-type": "application/json",
                "Access-Control-Allow-Origin": "*"
                },
            "body": labels,  
            "statusCode": 200
          }
    return out
