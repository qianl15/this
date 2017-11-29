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
from timeit import default_timer as now
import HTMLParser
html_parser = HTMLParser.HTMLParser()

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
DEFAULT_OUT_FOLDER = 'mxnet-results/'
    
#params
# start = now()
f_params_file = '/tmp/' + f_params
# urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-0000.params", f_params_file)

#symbol
f_symbol_file = '/tmp/' + f_symbol
# urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-symbol.json", f_symbol_file)
# end = now()
# print('Time to download MXNet model: {:.4f} s'.format(end - start))

def ensure_clean_state():
  if os.path.exists(LOCAL_IMG_PATH):
    os.remove(LOCAL_IMG_PATH)

def download_input_from_s3(bucketName, fileName, localfile=LOCAL_IMG_PATH):
  print('Downloading file {:s} from s3: {:s}'.format(fileName, bucketName))
  s3 = boto3.resource('s3')
  try:
    s3.Bucket(bucketName).download_file(fileName, localfile)
  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
      print("The object does not exist.")
    else:
      raise

def upload_output_to_s3(bucketName, fileName, out):
  print('Uploading file {:s} to s3: {:s}'.format(fileName, bucketName))
  s3 = boto3.client('s3')

  try:
    s3.put_object(Body=json.dumps(out), Bucket=bucketName, Key=fileName, 
                  StorageClass='REDUCED_REDUNDANCY')
  except botocore.exceptions.ClientError as e:
    print e
    raise
  print('Done: {:s}/{:s}'.format(bucketName, fileName))

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
  urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-0000.params", f_params_file)

  urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-symbol.json", f_symbol_file)
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
  timelist = "{"
  start = now()
  urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-0000.params", f_params_file)

  urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-symbol.json", f_symbol_file)
  end = now()
  print('Time to download MXNet model: {:.4f} s'.format(end - start))
  timelist += '"download-model" : %f,' % (end - start)

  inputBucket = 'vass-video-samples2'
  inputKey = 'batch-test/1901+100.jpg'
  batchSize = 1 # the batch passed to MXNet
  outputBucket = 'vass-video-samples2'
  outputKey = 'mxnet-results/1901+100.out'
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
          print('Warning: using default input bucket')
      if 'inputKey' in data:
          inputKey = data['inputKey']
      else:
          print('Warning: using default input file')
      if 'batchSize' in data:
          batchSize = data['batchSize']
      else:
          print('Warning: using default batch_size = 1')
      if 'outputBucket' in data:
          outputBucket = data['outputBucket']
      else:
          outputBucket = inputBucket + "-results"
          print('Warning: using default output bucket {:s}'.format(outputBucket))
      if 'outputKey' in data:
          outputKey = data['outputKey']
      else:
          outputKey = inputKey.split(".")[0].split("/")[-1] + '.out'
          outputKey = DEFAULT_OUT_FOLDER + outputKey
          print('Warning: using default output key {:s}'.format(outputKey))
  except KeyError:
    # direct invocation
    if 'inputBucket' in event:
        inputBucket = event['inputBucket']
    else:
        print('Warning: using default input bucket')
    if 'inputKey' in event:
        inputKey = event['inputKey']
    else:
        print('Warning: using default input file')
    if 'batchSize' in event:
        batchSize = event['batchSize']
    else:
        print('Warning: using default batch_size = 1')
    if 'outputBucket' in event:
        outputBucket = event['outputBucket']
    else:
        outputBucket = inputBucket + "-results"
        print('Warning: using default output bucket {:s}'.format(outputBucket))
    if 'outputKey' in event:
        outputKey = event['outputKey']
    else:
        outputKey = inputKey.split(".")[0].split("/")[-1] + '.out'
        outputKey = DEFAULT_OUT_FOLDER + outputKey
        print('Warning: using default output key {:s}'.format(outputKey))


  start = now()
  download_input_from_s3(inputBucket, inputKey)
  end = now()
  print('Time to download input file: {:.4f} s'.format(end - start))
  timelist += '"download-input" : %f,' % (end - start)

  start = now()
  data = one_file_to_many(LOCAL_IMG_PATH)
  end = now()
  count = len(data)
  print('Time to extract {:d} file: {:.4f} s'.format(count, end - start))
  timelist += '"extract" : %f,' % (end - start)
  if (count % batchSize) != 0:
    print('input files number {:d} cannot be divided by '.format(count) +  
        'batch size {:d}'.format(batchSize))
    exit()

  start = now()
  sym, arg_params, aux_params = load_model(f_symbol_file, f_params_file)
  mod = mx.mod.Module(symbol=sym, label_names=None)
  mod.bind(for_training=False, data_shapes=[('data', (batchSize,3,224,224))],
          label_shapes=mod._label_shapes)
  mod.set_params(arg_params, aux_params, allow_missing=True)
  end = now()
  print('Time to prepare and load parameters: {:.4f} s'.format(end - start))
  timelist += '"load" : %f,' % (end - start)
  start = now()
  labels = predict_batch(batchSize, data, mod)
  end = now()
  print('Time to predict the {:d} batch: {:.4f} s'.format(batchSize, end -
     start))
  timelist += '"predict" : %f' % (end - start)

  timelist += "}"
  out = {
      "headers": {
          "content-type": "application/json",
          "Access-Control-Allow-Origin": "*"
          },
      "body": {
          "results": labels,  
          "times": timelist},
      "statusCode": 200
  }
  print timelist

  upload_output_to_s3(outputBucket, outputKey, out)

  return out


# link with S3 event
def lambda_s3_batch_handler(event, context):
  ensure_clean_state()
  inputBucket = 'vass-video-samples2'
  inputKey = 'batch-test/1901+100.jpg'
  batchSize = 50 # the batch passed to MXNet, cannot be passed through s3 event
  outputBucket = 'vass-video-samples2-results'
  outputKey = 'mxnet-results/1901-100.out'

  timelist = "{"
  start = now()
  urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-0000.params", f_params_file)

  urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-symbol.json", f_symbol_file)
  end = now()
  print('Time to download MXNet model: {:.4f} s'.format(end - start))
  timelist += '"download-model" : %f,' % (end - start)
  
  for record in event['Records']:
    inputBucket = html_parser.unescape(record['s3']['bucket']['name'])
    inputKey = html_parser.unescape(record['s3']['object']['key'])
    outputBucket = inputBucket + "-results"
    # outputKey = inputKey.split(".")[0].split("/")[-1] + '.out'
    tmpKeyList = inputKey.split(".")[0].split("/")[-2:]
    outputKey = DEFAULT_OUT_FOLDER + '/'.join(tmpKeyList) + '.out'
    print('Outputkey is: {}'.format(outputKey))

    start = now()
    download_input_from_s3(inputBucket, inputKey, LOCAL_IMG_PATH)
    end = now()
    inputSize = os.path.getsize(LOCAL_IMG_PATH)
    print('Time to download input file: {:.4f} s, size {} KB'.format(
      end - start, inputSize))
    timelist += '"download-input" : %f,' % (end - start)

    start = now()
    data = one_file_to_many(LOCAL_IMG_PATH)
    end = now()
    count = len(data)
    print('Time to extract {:d} file: {:.4f} s'.format(count, end - start))
    timelist += '"extract-input" : %f,' % (end - start)
    if (count % batchSize) != 0:
      print('input files number {:d} cannot be divided by '.format(count) +  
          'batch size {:d}'.format(batchSize))
      # exit()
      if count < 100:
        batchSize = count
      else:
        batchSize = 1
      print('Using batch size: {:d}'.format(batchSize))

    start = now()
    sym, arg_params, aux_params = load_model(f_symbol_file, f_params_file)
    mod = mx.mod.Module(symbol=sym, label_names=None)
    mod.bind(for_training=False, data_shapes=[('data', (batchSize,3,224,224))],
            label_shapes=mod._label_shapes)
    mod.set_params(arg_params, aux_params, allow_missing=True)
    end = now()
    print('Time to prepare and load parameters: {:.4f} s'.format(end - start))
    timelist += '"load-model" : %f,' % (end - start)
  
    start = now()
    labels = predict_batch(batchSize, data, mod)
    end = now()
    print('Time to predict the {:d} batch: {:.4f} s'.format(batchSize, end -
       start))
    timelist += '"predict" : %f,' % (end - start)
    
    start = now()
    out = {
        "results": labels
    }
    upload_output_to_s3(outputBucket, outputKey, out)
    end = now()

    print('Time to upload to s3 is: {:.4f} s'.format(end - start))
    timelist += '"upload-output" : %f,' % (end - start)
    timelist += '"batch" : %d' % (batchSize)
    timelist += "}"
    
    print 'Timelist:' + json.dumps(timelist)

# for local test
if __name__ == '__main__':
  start = now()
  inputBucket = 'vass-video-samples2'
  inputKey = 'batch-test/1901+100.jpg'
  batchSize = 1
  outputBucket = 'vass-video-samples2'
  outputKey = 'mxnet-results/1901+100.out'
  if (len(sys.argv) > 1):
    batchSize = int(sys.argv[1])
    if (len(sys.argv) > 2):
      inputBucket = sys.argv[2]
      outputBucket = inputBucket
    if (len(sys.argv) > 3):
      inputKey = sys.argv[3]
      outputKey = inputKey.split(".")[0].split("/")[-1] + '.out'
      outputKey = DEFAULT_OUT_FOLDER + outputKey
    if (len(sys.argv) > 4):
      outputBucket = sys.argv[4]
    if (len(sys.argv) > 5):
      outputBucket = sys.argv[5]
  event = {
      'inputBucket': inputBucket,
      'inputKey': inputKey,
      'batchSize': batchSize,
      'outputBucket': outputBucket,
      'outputKey': outputKey
  }
  print event
  out = lambda_batch_handler(event, {})
  end = now()
  print('Total time: {:.4f}'.format(end - start))
  #print out
