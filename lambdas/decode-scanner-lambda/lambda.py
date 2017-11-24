import os
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

DECODER_PATH = '/tmp/DecoderAutomataCmd-static'
TEMP_OUTPUT_DIR = '/tmp/output'

shutil.copy('DecoderAutomataCmd-static', DECODER_PATH)
os.chmod(DECODER_PATH, 0o755)

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
  return sorted(outputFiles)

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
  print 'Wrote', outPath

def combine_output_files(outputBatchSize):
  def encode_batch(batch):
    inputFilePaths = [
      os.path.join(TEMP_OUTPUT_DIR, x) for x in batch
    ]
    name, ext = os.path.splitext(batch[0])
    outputFilePath = os.path.join(
      TEMP_OUTPUT_DIR, '%s-%d%s' % (name, len(batch), ext))
    many_files_to_one(inputFilePaths, outputFilePath)
    for filePath in inputFilePaths:
      os.remove(filePath)

  currentBatch = []
  for fileName in list_output_files():
    currentBatch.append(fileName)
    if len(currentBatch) == outputBatchSize:
      encode_batch(currentBatch)
      currentBatch = []

  if len(currentBatch) > 0: 
    encode_batch(currentBatch)

def upload_output_to_s3(bucketName, filePrefix):
  print 'Uploading files to s3: %s' % bucketName
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


def handler(event, context):
  ensure_clean_state()

  outputBucket = "vass-video-samples2-results"
  outputPrefix = "decode-test"
  outputBatchSize = 1

  assert(os.path.exists(TEMP_OUTPUT_DIR))

  cmd = [DECODER_PATH, 'decode_args0.proto', 'start_frame0.bin', TEMP_OUTPUT_DIR]
  process = subprocess.Popen(
    ' '.join(cmd), shell=True,
    stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE)
  out, err = process.communicate()
  rc = process.returncode

  fileCount, totalSize = upload_output_to_s3(outputBucket, outputPrefix)
  shutil.rmtree(TEMP_OUTPUT_DIR)
  return { 'out': out, 'err': err, 'retcode': rc }
