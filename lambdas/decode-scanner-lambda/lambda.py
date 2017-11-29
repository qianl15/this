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
import urllib
from timeit import default_timer as now
import json

DECODER_PATH = '/tmp/DecoderAutomataCmd-static'
TEMP_OUTPUT_DIR = '/tmp/output'
LOCAL_INPUT_DIR = '/tmp/input'


# os.environ['LD_LIBRARY_PATH'] = '$%s:%s/scanner/' % (os.environ['LD_LIBRARY_PATH'], os.getcwd())
DEFAULT_LOG_LEVEL = 'warning'

DEFAULT_OUTPUT_BATCH_SIZE = 1
DEFAULT_KEEP_OUTPUT = False

MAX_PARALLEL_UPLOADS = 20

OUTPUT_FILE_EXT = 'jpg'

def list_output_files():
  fileExt = '.{0}'.format(OUTPUT_FILE_EXT)
  outputFiles = [
    x for x in os.listdir(TEMP_OUTPUT_DIR) if x.endswith(fileExt)
  ]
  return outputFiles

def many_files_to_one(inPaths, outPath):
  with open(outPath, 'wb') as ofs:
    for filePath in inPaths:
      with open(filePath, 'rb') as ifs:
        data = ifs.read()
        dataLen = len(data)
        fileName = os.path.basename(filePath)
        ofs.write(struct.pack('I', len(fileName)))
        ofs.write(fileName)
        ofs.write(struct.pack('I', dataLen))
        ofs.write(data)
      os.remove(filePath) # delete here!
  print 'Wrote', outPath

def combine_output_files(startFrame, outputBatchSize):
  def encode_batch(batch):
    inputFilePaths = [
      os.path.join(TEMP_OUTPUT_DIR, x) for x in batch
    ]
    name, ext = os.path.splitext(batch[0])
    outputFilePath = os.path.join(
      TEMP_OUTPUT_DIR, '%s-%d%s' % (name, len(batch), ext))
    many_files_to_one(inputFilePaths, outputFilePath)
    # for filePath in inputFilePaths:
    #   os.remove(filePath)

  # Guarantee the sequence and order! otherwise, "20" > "100" because of
  # the character order
  currentBatch = []
  outputFiles = list_output_files()
  totalNum = len(outputFiles)
  remain = totalNum
  for currStart in xrange(startFrame, startFrame + totalNum, outputBatchSize):
    currEnd = min(remain, outputBatchSize) + currStart
    for idx in xrange(currStart, currEnd):
      fileName = 'frame{:d}.jpg'.format(idx)
      if fileName not in outputFiles:
        print('ERROR: Cannot find file: {:s}'.format(fileName))
        exit()
      currentBatch.append(fileName)
    encode_batch(currentBatch)
    currentBatch = []
    remain -= outputBatchSize
  return totalNum

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

def upload_output_to_s3(bucketName, filePrefix):
  print('Uploading files to s3: {:s}/{:s}'.format(bucketName, filePrefix))
  s3 = boto3.client('s3', config=botocore.client.Config(
    max_pool_connections=MAX_PARALLEL_UPLOADS))

  count = 0
  totalSize = 0
  results = []

  pool = ThreadPool(MAX_PARALLEL_UPLOADS)
  sema = Semaphore(MAX_PARALLEL_UPLOADS)

  def upload_file(localFilePath, uploadFileName, fileSize):
    sema.acquire()
    try:
      print 'Start: %s [%dKB]' % (localFilePath, fileSize >> 10)
      with open(localFilePath, 'rb') as ifs:
        s3.put_object(Body=ifs, Bucket=bucketName,
          Key=uploadFileName,
          StorageClass='REDUCED_REDUNDANCY')
      print 'Done: %s' % localFilePath
    finally:
      sema.release()

  for fileName in list_output_files():
    localFilePath = os.path.join(TEMP_OUTPUT_DIR, fileName)
    uploadFileName = os.path.join(filePrefix, fileName)
    fileSize = os.path.getsize(localFilePath)

    result = pool.apply_async(upload_file, 
      args=(localFilePath, uploadFileName, fileSize))
    results.append(result)

    count += 1
    totalSize += fileSize

  # block until all threads are done
  for result in results:
    result.get()

  # block until all uploads are finished
  for _ in xrange(MAX_PARALLEL_UPLOADS):
    sema.acquire()

  print 'Uploaded %d files to S3 [total=%dKB]' % (count, totalSize >> 10)
  return (count, totalSize)

def list_output_directory():
  print '%s/' % TEMP_OUTPUT_DIR
  count = 0
  totalSize = 0
  for fileName in list_output_files():
    localFilePath = os.path.join(TEMP_OUTPUT_DIR, fileName)
    fileSize = os.path.getsize(localFilePath)
    print ' [%04dKB] %s' % (fileSize >> 10, fileName)
    totalSize += fileSize
    count += 1
  print 'Generated %d files [total=%dKB]' % (count, totalSize >> 10)
  return (count, totalSize)

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

def handler(event, context):
  timelist = "{"
  start = now()
  ensure_clean_state()
  end = now()
  print('Time to prepare decoder: {:.4f} s'.format(end - start))
  timelist += '"prepare-decoder" : %f,' % (end - start)

  inputBucket = 'vass-video-samples2'
  inputPrefix = 'protobin/example3_134'
  startFrame = 0
  outputBatchSize = 1

  outputBucket = "vass-video-samples2-results"
  outputPrefix = "decode-output"
  
  if 'inputBucket' in event:
    inputBucket = event['inputBucket']
    # outputBucket = inputBucket + '-results'
    outputBucket = inputBucket
  else:
    print('Warning: using default input bucket: {:s}'.format(inputBucket))
  if 'inputPrefix' in event:
    inputPrefix = event['inputPrefix']
    # get the video name!
    # outputPrefix = outputPrefix + '/' + inputPrefix.split('/')[-1] 
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

  outputPrefix = outputPrefix + '/{}_{}'.format(inputPrefix.split('/')[-1], 
                                                outputBatchSize)

  start = now()
  protoPath, binPath = download_input_from_s3(inputBucket, inputPrefix, 
                                              startFrame)
  end = now()
  print('Time to download input files: {:.4f} s'.format(end - start))
  timelist += '"download-input" : %f,' % (end - start)

  inputBatch = 0
  try:
    try:
      start = now()
      if not convert_to_jpegs(protoPath, binPath):
        raise Exception('Failed to decode video chunk {:d}'.format(startFrame))
      end = now()
      print('Time to decode: {:.4f} '.format(end - start))
      timelist += '"decode" : %f,' % (end - start)
    finally:
      shutil.rmtree(LOCAL_INPUT_DIR)

    start = now()
    if outputBatchSize > 1:
      inputBatch = combine_output_files(startFrame, outputBatchSize)
    end = now()
    print('Time to combine output files: {:.4f} '.format(end - start))
    timelist += '"combine-output" : %f,' % (end - start)

    start = now()
    fileCount, totalSize = upload_output_to_s3(outputBucket, outputPrefix)
    end = now()
    if outputBatchSize == 1:
      inputBatch = fileCount
    print('Time to upload output files: {:.4f} '.format(end - start))
    timelist += '"upload-output" : %f,' % (end - start)
  finally:
    start = now()
    if not DEFAULT_KEEP_OUTPUT:
      shutil.rmtree(TEMP_OUTPUT_DIR)
    end = now()
    print('Time to clean output files: {:.4f} '.format(end - start))
    timelist += '"clean-output" : %f,' % (end - start)
  
  timelist += '"input-batch" : %d' % (inputBatch)
  timelist += '}'

  print 'Timelist:' + json.dumps(timelist)
  out = {
    'statusCode': 200,
    'body': {
      'fileCount': fileCount,
      'totalSize': totalSize
    }
  }
  return out


if __name__ == '__main__':
  inputBucket = 'vass-video-samples2'
  inputPrefix = 'protobin/example3_135'
  startFrame = 0
  outputBatchSize = 50

  if (len(sys.argv) > 1):
    startFrame = int(sys.argv[1])

  event = {
    'inputBucket': inputBucket,
    'inputPrefix': inputPrefix,
    'startFrame': startFrame,
    'outputBatchSize': outputBatchSize
  }
  print event

  out = handler(event, {})
  print out
