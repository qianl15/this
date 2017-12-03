import os
import sys
import shutil
import subprocess
import boto3
import botocore
import hashlib
import struct 
from multiprocessing.pool import ThreadPool
from threading import Semaphore
import urllib2
from urllib import urlretrieve
from timeit import default_timer as now
import json
from collections import OrderedDict
import math

import mxnet as mx
import numpy as np

from PIL import Image
from io import BytesIO
import os.path

f_params = 'resnet-18-0000.params'
f_symbol = 'resnet-18-symbol.json'
f_params_file = '/tmp/' + f_params
f_symbol_file = '/tmp/' + f_symbol

DECODER_PATH = '/tmp/DecoderAutomataCmd-static'
TEMP_OUTPUT_DIR = '/tmp/output'
LOCAL_INPUT_DIR = '/tmp/input'
WORK_PACKET_SIZE = 50

DEFAULT_LOG_LEVEL = 'warning'

DEFAULT_OUTPUT_BATCH_SIZE = 1
DEFAULT_KEEP_OUTPUT = False

MAX_PARALLEL_UPLOADS = 20

DEFAULT_OUT_FOLDER = 'fused-decode-mxnet-output'

OUTPUT_FILE_EXT = 'jpg'

def list_output_files():
  fileExt = '.{0}'.format(OUTPUT_FILE_EXT)
  outputFiles = [
    x for x in os.listdir(TEMP_OUTPUT_DIR) if x.endswith(fileExt)
  ]
  return outputFiles

def get_mxnet_input(startFrame):
  data = []

  outputFiles = list_output_files()
  totalNum = len(outputFiles)
  currEnd = totalNum + startFrame
  for idx in xrange(startFrame, currEnd):
    fileName = 'frame{:d}.jpg'.format(idx)
    if fileName not in outputFiles:
      print('ERROR: Cannot find file: {:s}'.format(fileName))
      exit()
    filePath = os.path.join(TEMP_OUTPUT_DIR, fileName)
    with open(filePath, 'rb') as ifs:
      data.append(ifs.read())
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

def download_input_from_s3(bucketName, inputPrefix, startFrame):
  def download_s3(s3Path, localPath):
    try:
      s3.Bucket(bucketName).download_file(s3Path, localPath)
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
      else:
        raise

  protoFileName = 'decode_args{:d}.proto'.format(startFrame)
  binFileName = 'start_frame{:d}.bin'.format(startFrame)
  print('Downloading files {:s} and {:s} for batch {:d} \
        from s3: {:s}/{:s}'.format(protoFileName, binFileName, startFrame, 
          bucketName, inputPrefix))
  s3 = boto3.resource('s3')
  s3ProtoName = inputPrefix + '/' + protoFileName
  s3BinName = inputPrefix + '/' + binFileName
  protoPath = LOCAL_INPUT_DIR + '/' + protoFileName
  binPath = LOCAL_INPUT_DIR + '/' + binFileName
  
  download_s3(s3ProtoName, protoPath)
  download_s3(s3BinName, binPath)

  return protoPath, binPath

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


def ensure_clean_state():
  if os.path.exists(TEMP_OUTPUT_DIR):
    shutil.rmtree(TEMP_OUTPUT_DIR)
  if not os.path.exists(TEMP_OUTPUT_DIR):
    os.mkdir(TEMP_OUTPUT_DIR)
  if os.path.exists(LOCAL_INPUT_DIR):
    shutil.rmtree(LOCAL_INPUT_DIR)
  if not os.path.exists(LOCAL_INPUT_DIR):
    os.mkdir(LOCAL_INPUT_DIR)

  if os.path.exists(DECODER_PATH):
    os.remove(DECODER_PATH)
  shutil.copy('DecoderAutomataCmd-static', DECODER_PATH)
  os.chmod(DECODER_PATH, 0o755)

def convert_to_jpegs(protoPath, binPath):
  assert(os.path.exists(TEMP_OUTPUT_DIR))
  cmd = [DECODER_PATH, protoPath, binPath, TEMP_OUTPUT_DIR]
  process = subprocess.Popen(
    ' '.join(cmd), shell=True,
    stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE)
  out, err = process.communicate()
  rc = process.returncode
  print 'stdout:', out
  print 'stderr:', err
  return rc == 0


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

def handler(event, context):
  timelist = OrderedDict()
  start = now()
  ensure_clean_state()
  end = now()
  print('Time to prepare decoder: {:.4f} s'.format(end - start))
  timelist["prepare-decoder"] = (end - start)

  inputBucket = 'vass-video-samples2'
  inputPrefix = 'protobin/example3_134'
  startFrame = 0
  outputBatchSize = 50

  outputBucket = "vass-video-samples2-results"
  outputPrefix = DEFAULT_OUT_FOLDER
  
  if 'inputBucket' in event:
    inputBucket = event['inputBucket']
    outputBucket = inputBucket + '-results'
  else:
    print('Warning: using default input bucket: {:s}'.format(inputBucket))
  if 'inputPrefix' in event:
    inputPrefix = event['inputPrefix']
  else:
    print('Warning: using default input prefix: {:s}'.format(inputPrefix))
  if 'startFrame' in event:
    startFrame = event['startFrame']
  else:
    print('Warning: default startFrame: {:d}'.format(startFrame))
  if 'outputBatchSize' in event:
    outputBatchSize = event['outputBatchSize']
  else:
    print('Warning: default batch size: {:d}'.format(outputBatchSize))
  if 'outputPrefix' in event:
    outputPrefix = event['outputPrefix']

  outputPrefix = outputPrefix + '/{}_{}'.format(inputPrefix.split('/')[-1], 
                                                outputBatchSize)

  start = now()
  protoPath, binPath = download_input_from_s3(inputBucket, inputPrefix, 
                                              startFrame)
  end = now()
  print('Time to download input files: {:.4f} s'.format(end - start))
  timelist["download-input"] = (end - start)

  inputBatch = 0
  try:
    try:
      start = now()
      if not convert_to_jpegs(protoPath, binPath):
        raise Exception('Failed to decode video chunk {:d}'.format(startFrame))
      end = now()
      print('Time to decode: {:.4f} '.format(end - start))
      timelist["decode"] = (end - start)
    finally:
      shutil.rmtree(LOCAL_INPUT_DIR)

    # start = now()
    # if outputBatchSize > 1:
    #   inputBatch = combine_output_files(startFrame, outputBatchSize)
    # end = now()
    # print('Time to combine output files: {:.4f} '.format(end - start))
    # timelist["combine-output"] = (end - start)

    # start = now()
    # fileCount, totalSize = upload_output_to_s3(outputBucket, outputPrefix)
    # end = now()
    # if outputBatchSize == 1:
    #   inputBatch = fileCount
    # print('Time to upload output files: {:.4f} '.format(end - start))
    # timelist["upload-output"] = (end - start)

    # instead of uploading, now we start the MXNet directly!
    start = now()
    urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-0000.params", f_params_file)

    urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-symbol.json", f_symbol_file)
    end = now()
    print('Time to download MXNet model: {:.4f} s'.format(end - start))
    timelist["download-model"] = (end - start)

    start = now()
    data = get_mxnet_input(startFrame)
    outputBatchSize = len(data)
    end = now()
    print('Time to extract {:d} file: {:.4f} s'.format(outputBatchSize, end - start))
    timelist["extract"] = (end - start)

    start = now()
    sym, arg_params, aux_params = load_model(f_symbol_file, f_params_file)
    mod = mx.mod.Module(symbol=sym, label_names=None)
    mod.bind(for_training=False, data_shapes=[('data', (outputBatchSize,3,224,224))],
            label_shapes=mod._label_shapes)
    mod.set_params(arg_params, aux_params, allow_missing=True)
    end = now()
    print('Time to prepare and load parameters: {:.4f} s'.format(end - start))
    timelist["load-model"] = end - start

    start = now()
    labels = predict_batch(outputBatchSize, data, mod)
    end = now()
    print('Time to predict the {:d} batch: {:.4f} s'.format(outputBatchSize, 
      end - start))
    timelist["predict"] = end - start

    start = now()
    outputKey = os.path.join(outputPrefix, 'frame{:d}-{:d}.out'.format(
      startFrame, outputBatchSize))
    out = {
        "results": labels
    }
    upload_output_to_s3(outputBucket, outputKey, out)
    end = now()
    print('Time to upload to s3 is: {:.4f} s'.format(end - start))
    timelist["upload-output"] = end - start

  finally:
    start = now()
    if not DEFAULT_KEEP_OUTPUT:
      shutil.rmtree(TEMP_OUTPUT_DIR)
    end = now()
    print('Time to clean output files: {:.4f} '.format(end - start))
    timelist["clean-output"] = (end - start)
  
  # timelist["input-batch"] = inputBatch
  timelist["output-batch"] = outputBatchSize

  print 'Timelist:' + json.dumps(timelist)
  out = {
    'statusCode': 200,
    'body': {
      'startFrame': startFrame,
      'outputBatchSize': outputBatchSize
    }
  }
  return out


if __name__ == '__main__':
  inputBucket = 'vass-video-samples2'
  inputPrefix = 'protobin/example3_138_50'
  startFrame = 0
  outputBatchSize = 50
  outputPrefix = 'fused-decode-mxnet-output'
  totalFrame = 6221

  if (len(sys.argv) > 1):
    totalFrame = min(int(sys.argv[1]), totalFrame)

  for startFrame in xrange(0, totalFrame, WORK_PACKET_SIZE):
    event = {
      'inputBucket': inputBucket,
      'inputPrefix': inputPrefix,
      'startFrame': startFrame,
      'outputBatchSize': outputBatchSize,
      'outputPrefix': outputPrefix
    }
    start = now()
    result = handler(event, {})
    end = now()
    duration = (end - start) * 1000
    billedDuration = math.ceil(duration / 100.0) * 100.0
    print('Duration: {:.2f} ms Billed Duration: {:.0f} ms   Memory Size: 1536 MB  Max Memory Used: 1536 MB'.format(duration, billedDuration))
