'''
Reference code to showcase MXNet model prediction on AWS Lambda 

@author: Sunil Mallya (smallya@amazon.com)
version: 0.2
'''

import os
import boto3
import botocore
import json
# import tempfile
import urllib2 
from urllib import urlretrieve
import struct
import sys

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
LOCAL_IMG_PATH = os.path.join('/tmp', 'local.jpg')
    
#params

f_params_file = '/tmp/' + f_params
urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-0000.params", f_params_file)

#symbol
f_symbol_file = '/tmp/' + f_symbol
urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-symbol.json", f_symbol_file)

def ensure_clean_state():
    if os.path.exists(LOCAL_IMG_PATH):
        os.remove(LOCAL_IMG_PATH)

def download_input_from_s3(bucketName, fileName):
    print('Downloading file {:s} from s3: {:s}'.format(fileName, bucketName))
    s3 = boto3.resource('s3')
    try:
        s3.Bucket(bucketName).download_file(fileName, LOCAL_IMG_PATH)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

def one_file_to_many(inPath):
    data = []
    with open(inPath, 'rb') as ifs:
        count = 0
        while True:
            chunk = ifs.read(4)
            if chunk == '':
                break
            fileNameLen = struct.unpack('I', chunk)[0]
            fileName = ifs.read(fileNameLen)
            chunk = ifs.read(4)
            if chunk == '':
                raise Exception('Expected 4 bytes')
            dataLen = struct.unpack('I', chunk)[0]
            data.append(ifs.read(dataLen))
            count += 1
        print('Extracted {:d} files'.format(count))
    return data

    
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
    prob = mod.get_outputs()[0].asnumpy() # 0 means device 0? prob can be
                                          # multiple batched values
    prob = np.squeeze(prob)
    a = np.argsort(prob)[::-1]
    
    # just return the index, not the synset!
    out = '{"0" : {"%s" : "%s"}' %(a[0], prob[a[0]]) 
    cnt = 0;
    for i in a[1:5]:
        cnt += 1;
        out += ', "%d" : {"%s" : "%s"}' %(cnt, i, prob[i])
    out += "}"

    return out

def predict_batch(batch_size, data, mod):
    '''
    predict labels for a given batch of images
    '''
    data_size = len(data)
    cnt = 0
    new_width, new_height = 224, 224
    out = "{"
    while cnt < data_size:
        # execute one batch
        img_list = []
        for frame in data[cnt:cnt+batch_size]:
            img = Image.open(BytesIO(frame))
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

        batch = mx.io.DataBatch([mx.nd.array(img_list)], [])
        mod.forward(batch)
        probs = mod.get_outputs()[0].asnumpy()
        print probs.shape

        cnt_local = cnt
        # the output format is : first is the relative id of the frame
        # then the second.first is the category (num), second.second is the
        # probability / confidence of the category
        # Be aware that this is different from previous version!
        for prob in probs:
            prob = np.squeeze(prob)
            a = np.argsort(prob)[::-1]
            if cnt_local == 0:
                out += '"0" : {{"{}" : "{}"}}'.format(a[0], prob[a[0]])
            else:
                out += ', "{:d}" : {{"{}" : "{}"}}'.format(cnt_local, 
                                                            a[0], prob[a[0]])
            cnt_local += 1

        cnt += batch_size

    out += "}"
    return out

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
    mod.bind(for_training=False, data_shapes=[('data', (1,3,224,224))],
       label_shapes=mod._label_shapes)
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

# fetch a batch of files from the S3 bucket
# also support the batch mode predict in MXNet
def lambda_batch_handler(event, context):
    ensure_clean_state()
    inputBucket = 'vass-video-samples2'
    inputKey = 'batch-test/1901+100.jpg'
    batchSize = 1 # the batch passed to MXNet
    try:
        # API Gateway GET method
        if event['httpMethod'] == 'GET':
            url = event['queryStringParameters']['url']
        # API Gateway POST method
        elif event['httpMethod'] == 'POST':
            data = json.loads(event['body'])
            if 'inputBucket' in data:
                inputBucket = data['inputBucket']
            else:
                print('Warning: using default bucket')
            if 'inputKey' in data:
                inputKey = data['inputKey']
            else:
                print('Warning: using default input file')
            if 'batchSize' in data:
                batchSize = data['batchSize']
            else:
                print('Warning: using default batch_size = 1')
    except KeyError:
        # direct invocation
        if 'inputBucket' in event:
            inputBucket = event['inputBucket']
        else:
            print('Warning: using default bucket')
        if 'inputKey' in event:
            inputKey = event['inputKey']
        else:
            print('Warning: using default input file')
        if 'batchSize' in event:
            batchSize = event['batchSize']
        else:
            print('Warning: using default batch_size = 1')

    download_input_from_s3(inputBucket, inputKey)
    data = one_file_to_many(LOCAL_IMG_PATH)
    count = len(data)
    if (count % batchSize) != 0:
        print('input files number {:d} cannot be divided by '.format(count) +  
            'batch size {:d}'.format(batchSize))
        exit()

    sym, arg_params, aux_params = load_model(f_symbol_file, f_params_file)
    mod = mx.mod.Module(symbol=sym, label_names=None)
    mod.bind(for_training=False, data_shapes=[('data', (batchSize,3,224,224))],
            label_shapes=mod._label_shapes)
    mod.set_params(arg_params, aux_params, allow_missing=True)
    labels = predict_batch(batchSize, data, mod)

    out = {
            "headers": {
                "content-type": "application/json",
                "Access-Control-Allow-Origin": "*"
                },
            "body": labels,  
            "statusCode": 200
    }
    return out


# for local test
if __name__ == '__main__':
    inputBucket = 'vass-video-samples2'
    inputKey = 'batch-test/1901+100.jpg'
    batchSize = 1
    if (len(sys.argv) > 1):
      batchSize = int(sys.argv[1])
      if (len(sys.argv) > 2):
        inputBucket = sys.argv[2]
      if (len(sys.argv) > 3):
        inputKey = sys.argv[3]
    event = {
        'inputBucket': inputBucket,
        'inputKey': inputKey,
        'batchSize': batchSize
    }
    out = lambda_batch_handler(event, {})
    print out
