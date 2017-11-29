######################################################
# -*- coding: utf-8 -*-
# File Name: end2end_mxnet.py
# Author: Qian Li
# Created Date: 2017-11-24
# Description: The end-to-end system code
# We can decode & evaluate MXNet on Lambda!
######################################################

from scannerpy import Database, Job, ColumnType, DeviceType, BulkJob
from scannerpy.stdlib import parsers
import sys
import os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../tests')
import util
from timeit import default_timer as now
import time
import math
from urllib import urlretrieve
import boto3
import botocore
from multiprocessing.pool import ThreadPool
from threading import Semaphore, Lock
import progressbar
import json

import logging
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

WORK_PACKET_SIZE = 50  # how many frames to decode together
BATCH_SIZE = 50 # how many frames to be evaluated together
DEFAULT_KEEP_OUTPUT = False
MAX_PARALLEL_UPLOADS = 20

UPLOAD_BUCKET = 'vass-video-samples2'
UPLOAD_PREFIX = 'protobin'

DOWNLOAD_BUCKET = 'vass-video-samples2-results'
DOWNLOAD_PREFIX = 'mxnet-results'

DEFAULT_OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
PROTO_EXT = 'proto'
BIN_EXT = 'bin'
OUT_EXT = 'out'

TIMEOUT_SECONDS = 300.0 # maximum wait time

timelist = "{"

def list_output_files(outputDir = './', fileExt = None):
  if fileExt == None:
    print('Please provide file extension: e.g., .jpg, .bin')
    exit()
  fileExt = '.{0}'.format(fileExt)
  # print('output dir: {:s}, fileExt: {:s}'.format(outputDir, fileExt))
  outputFiles = [
    x for x in os.listdir(outputDir) if x.endswith(fileExt)
  ]
  return sorted(outputFiles)

# Upload all files with certain extension to a bucket
uploadFileCount = 0
def upload_output_to_s3(bucketName, filePrefix, fileExt):
  print('Uploading files to s3: {:s}/{:s}'.format(bucketName, filePrefix))
  s3 = boto3.client('s3', config=botocore.client.Config(
    max_pool_connections=MAX_PARALLEL_UPLOADS))
  
  global uploadFileCount
  uploadFileCount = 0
  countLock = Lock()
  totalSize = 0
  results = []

  pool = ThreadPool(MAX_PARALLEL_UPLOADS)
  sema = Semaphore(MAX_PARALLEL_UPLOADS)

  maxval = sum(1 for _ in list_output_files(DEFAULT_OUTPUT_DIR, fileExt))

  bar = progressbar.ProgressBar(maxval=maxval, \
    widgets=[progressbar.Bar('=', 'Uploaded  [', ']'), ' ',
             progressbar.Percentage()])
  bar.start()

  def upload_file(localFilePath, uploadFileName, fileSize):

    sema.acquire()
    try:
      # print 'Start: %s [%dKB]' % (localFilePath, fileSize >> 10)
      with open(localFilePath, 'rb') as ifs:
        s3.put_object(Body=ifs, Bucket=bucketName,
          Key=uploadFileName,
          StorageClass='REDUCED_REDUNDANCY')
      # print 'Done: %s' % localFilePath
    finally:
      sema.release()
      with countLock:
        global uploadFileCount
        uploadFileCount += 1
        bar.update(uploadFileCount)

  for fileName in list_output_files(DEFAULT_OUTPUT_DIR, fileExt):
    localFilePath = os.path.join(DEFAULT_OUTPUT_DIR, fileName)
    uploadFileName = os.path.join(filePrefix, fileName)
    fileSize = os.path.getsize(localFilePath)

    result = pool.apply_async(upload_file, 
      args=(localFilePath, uploadFileName, fileSize))
    results.append(result)

    totalSize += fileSize
    
  # block until all threads are done
  for result in results:
    result.get()

  # block until all uploads are finished
  for _ in xrange(MAX_PARALLEL_UPLOADS):
    sema.acquire()
  bar.finish()

  print 'Uploaded %d files to S3 [total=%dKB]' % (uploadFileCount, totalSize >> 10)

  if DEFAULT_KEEP_OUTPUT == False:
    print('Deleting local output files...')
    for fileName in list_output_files(DEFAULT_OUTPUT_DIR, fileExt):
      localFilePath = os.path.join(DEFAULT_OUTPUT_DIR, fileName)
      # print localFilePath
      os.remove(fileName)
  return (uploadFileCount, totalSize)


def invoke_decoder_lambda(bucketName, filePrefix, startFrame, batchSize):
  client = boto3.client('lambda')
  payload = '{{ \"inputBucket\": \"{:s}\", \
    \"inputPrefix\": \"{:s}\", \
    \"startFrame\": {:d}, \
    \"outputBatchSize\": {:d}\
    }}'.format(bucketName, filePrefix, startFrame, batchSize)

  response = client.invoke(FunctionName='decoder-scanner',
                           InvocationType='Event',
                           Payload=str.encode(payload))

  if response['StatusCode'] == 202:
    return True
  else:
    return False

# Wait until all output files appear in S3 bucket, return # files
def wait_until_all_finished(startFrame, numRows, batch, videoPrefix):
  fileLists = []
  totalCount = len(xrange(startFrame, numRows, batch))
  s3 = boto3.resource('s3') # for method 1, 3
  # s3 = boto3.client('s3') # for method 2
  outputBucket = DOWNLOAD_BUCKET
  # remain = numRows
  # for currStart in xrange(startFrame, numRows, batch):
  #   currEnd = min(remain, batch)
  #   outputKey = '{}/{}_{}_{}/frame{}-{}.out'.format(DOWNLOAD_PREFIX, 
  #     videoPrefix, WORK_PACKET_SIZE, batch, currStart, currEnd)
  #   fileLists.append(outputKey)
  #   remain -= batch

  bar = progressbar.ProgressBar(maxval=totalCount, \
    widgets=[progressbar.Bar('=', 'Files     [', ']'), ' ', 
             progressbar.Percentage()])
  bar.start()

  fileCount = 0
  time.sleep(10.0) # sleep for 10 seconds to wait for decoder finished!
  startTime = now()
  timeOut = startTime + TIMEOUT_SECONDS
  while fileCount < totalCount:
    # for outputKey in fileLists:
      # method 1: load object
      # try:
      #   s3.Object(outputBucket, outputKey).load()
      # except botocore.exceptions.ClientError as e:
      #   if e.response['Error']['Code'] == "404":
      #     pass
      #   else:
      #     # Something else has gone wrong.
      #     raise
      # else:
      #   print('Output file {} found!'.format(outputKey))
      #   fileLists.remove(outputKey)
      #   fileCount += 1

      # method 2: head_object
      # try:
      #   s3.head_object(Bucket=outputBucket, Key=outputKey)
      # except botocore.exceptions.ClientError as e:
      #   if e.response['Error']['Code'] == "404":
      #     print('Cannot find file {}'.format(outputKey))
      #   else:
      #     # Something else has gone wrong.
      #     raise
      # else:
      #   print('Output file {} found!'.format(outputKey))
      #   fileLists.remove(outputKey)
      #   fileCount += 1

    # method 3: list the number of objects
    myBucket = s3.Bucket(DOWNLOAD_BUCKET)
    currCount = sum(1 for _ in myBucket.objects.filter(
      Prefix='{}/{}_{}_{}/'.format(DOWNLOAD_PREFIX, videoPrefix, 
                                   WORK_PACKET_SIZE, batch)))
    fileCount = currCount
    # print('fileCount is: {:d}'.format(fileCount))
    bar.update(fileCount)
    if fileCount >= totalCount:
      break

    currTime = now()
    if currTime >= timeOut:
      print('Timed out in {:.4f} sec, cannot finish.'.format(currTime - startTime))
      break
    # if currTime >= timeOut:
    #   break
    time.sleep(0.1)
  bar.finish()
  return fileCount

# choose which video we wanted to download, and the format
# format 134 = 360p, 135 = 480p, 136 = 720p, 137 = 1080p, 138 = 4k
# By default, we download the third video with the lowest quality
# batch - number of frames to do in a MXNet Lambda
def start_mxnet_pipeline(test_video_path='videos/example.mp4', 
                         out_dir = './', batch = BATCH_SIZE,
                         load_to_disk = False):
  global timelist

  if util.have_gpu():
    device = DeviceType.GPU
    print('with GPU device!')
  else:
    device = DeviceType.CPU
    print('only has CPU device!')

  script_dir = os.path.dirname(os.path.abspath(__file__))

  num_rows = 0
  # Start Scanner DB, use its load worker to generate .proto and .bin files
  with Database() as db:
    # register the fake kernel
    db.register_op('Fake', [('frame', ColumnType.Video)], ['class'])
    kernel_path = script_dir + '/fake_op.py'
    db.register_python_kernel('Fake', device, kernel_path, batch = 10)

    # Choose Fake kernel can be faster, or you can choose PyMxnet
    # db.register_op('PyMxnet', [('frame', ColumnType.Video)], ['class'])
    # kernel_path = script_dir + '/pymxnet_op.py'
    # db.register_python_kernel('PyMxnet', DeviceType.CPU, kernel_path, batch=10)

    start = now()
    [input_table], failed = db.ingest_videos([ 
        ('end2end_raw', test_video_path)], force=True)
    stop = now()
    delta = stop - start
    print('Time to ingest videos: {:.4f}s, fps: {:.4f}'.format(
      delta, input_table.num_rows() / delta))
    timelist += '"ingest-video" : %f,' % (delta)

    num_rows = input_table.num_rows()
    print('Number of frames in movie: {:d}'.format(num_rows))
    
    if len(failed) > 0:
      print('Failures:', failed)

    # Start to analyze the movie
    start = now()
    frame = db.ops.FrameInput()
    # Then we use our op just like in the other examples.
    # Choose Fake kernel can be faster, or you can choose PyMxnet 
    classes = db.ops.Fake(frame = frame, batch = batch)
    # classes = db.ops.PyMxnet(frame = frame, batch = batch)
    output_op = db.ops.Output(columns=[classes])
    job = Job(
      op_args={
        frame: input_table.column('frame'),
        output_op: 'end2end_out'
      }
    )
    bulk_job = BulkJob(output=output_op, jobs=[job])
    [output_table] = db.run(bulk_job, force=True, profiling=False, pipeline_instances_per_node=1, load_to_disk=load_to_disk, 
      work_packet_size=WORK_PACKET_SIZE)

    stop = now()
    delta = stop - start
    print('Batch: {:d} End-to-end Python Kernel time: {:.4f}s, {:.1f} fps\n'.format(batch, delta, input_table.num_rows() / delta))
    timelist += '"scanner-execution" : %f,' % (delta)

    # output_table.profiler().write_trace(
    #   out_dir + 'end2end_{:d}.trace'.format(batch))

    # If not load_to_disk, then it does not go to the next part
    if load_to_disk == False:
      video_classes = output_table.load(['class'], parsers.classes)

      # Loop over the column's rows. 
      # Each row is a tuple of the frame number and value for that row.
      num_rows = 0
      for (frame_index, frame_classes) in video_classes:
        assert len(frame_classes) == 1
        assert frame_classes[0].shape[0] == 1
        # print(frame_classes[0])
        num_rows += 1
      assert num_rows == db.table('end2end_raw').num_rows()

      print(db.summarize())
      exit()

  # Then start the Lambda part
  # extract video name
  videoPrefix = test_video_path.split(".")[-2].split("/")[-1]
  print('video name is: {:s}'.format(videoPrefix))
  # uploadPrefix = UPLOAD_PREFIX + '/' + videoPrefix
  uploadPrefix = UPLOAD_PREFIX + '/{}_{}'.format(videoPrefix, WORK_PACKET_SIZE)

  if load_to_disk == True:
    # Upload all .proto files
    start = now()
    fileCount, totalSize = upload_output_to_s3(
      UPLOAD_BUCKET, uploadPrefix, PROTO_EXT)

    # Upload all .bin files
    fileCount, totalSize = upload_output_to_s3(
      UPLOAD_BUCKET, uploadPrefix, BIN_EXT)
    stop = now()
    delta = stop - start
    print('Upload to S3 time: {:.4f} s'.format(delta))
    timelist += '"upload-s3" : %f,' % (delta)

    # Call Lambdas to decode, provide Bucket Name, File Prefix, Start Frame
    # Then decoder Lambdas will write to S3, which will trigger MXNet Lambdas
    start = now()
    lambdaTotalCount = len(xrange(0, num_rows, WORK_PACKET_SIZE))
    bar = progressbar.ProgressBar(maxval=lambdaTotalCount, \
          widgets=[progressbar.Bar('=', 'Lambdas   [', ']'), ' ',
                   progressbar.Percentage()])
    bar.start()
    lambdaCount = 0
    for startFrame in xrange(0, num_rows, WORK_PACKET_SIZE):
      # print("Invoke lambda for start frame {:d}".format(startFrame))
      result = invoke_decoder_lambda(UPLOAD_BUCKET, uploadPrefix, 
                                     startFrame, batch)
      if not result:
        print('Fail to invoke for frame {:d}, retry.'.format(startFrame))
        res = invoke_decoder_lambda(UPLOAD_BUCKET, uploadPrefix, 
                                    startFrame, batch)
        if not res:
          print('Frame {:d} still failed, exit'.format(startFrame))
          exit()
      lambdaCount += 1
      bar.update(lambdaCount)
    bar.finish()
    stop = now()
    delta = stop - start
    assert(lambdaCount == lambdaTotalCount)
    print('Triggered #{} Lambdas, time {:.4f} s'.format(lambdaCount, delta))
    timelist += '"invoke-lambda" : %f,' % (delta)

    # Wait until all output files appear
    fileCount = wait_until_all_finished(0, num_rows, batch, videoPrefix)
    # assert(fileCount == len(xrange(0, num_rows, batch)))
    totalCount = len(xrange(0, num_rows, batch)) 
    print('Collected {:d} out of {:d} files, error rate: {:.4f}'.format(fileCount, totalCount, 
        (totalCount - fileCount) * 1.0 / totalCount))

def ensure_clean_state(test_video_path, batch):
  print('Cleaning the folder')
  for fileName in list_output_files(DEFAULT_OUTPUT_DIR, PROTO_EXT):
      localFilePath = os.path.join(DEFAULT_OUTPUT_DIR, fileName)
      print('CLeaning: {}'.format(localFilePath))
      os.remove(fileName)

  for fileName in list_output_files(DEFAULT_OUTPUT_DIR, BIN_EXT):
      localFilePath = os.path.join(DEFAULT_OUTPUT_DIR, fileName)
      print('CLeaning: {}'.format(localFilePath))
      os.remove(fileName)

  
  videoPrefix = test_video_path.split(".")[-2].split("/")[-1]
  print('Cleaning S3 bucket: {}/{}/{}_{}_{}/'.format(DOWNLOAD_BUCKET, 
    DOWNLOAD_PREFIX, videoPrefix, WORK_PACKET_SIZE, batch))
  s3 = boto3.resource('s3')
  myBucket = s3.Bucket(DOWNLOAD_BUCKET)
  fileCount = 0
  for obj in myBucket.objects.filter(
      Prefix='{}/{}_{}_{}/'.format(DOWNLOAD_PREFIX, videoPrefix, 
                                   WORK_PACKET_SIZE, batch)):
    s3.Object(myBucket.name, obj.key).delete()
    fileCount += 1

  print('Deleted {} files'.format(fileCount))

if __name__ == '__main__':
  num = 1 # which video
  fm_num = 1 # which resolution
  out_dir = './' # which output directory
  batch = BATCH_SIZE
  load_to_disk = False;

  if (len(sys.argv) < 1) or (len(sys.argv) > 6):
    print('Usage: end2end_mxnet.py <video_num> <video_resolution> <out_dir> <batch_size> <load to disk: 0/1>');
    exit()

  if (len(sys.argv) > 1):
    num = int(sys.argv[1])
  if (len(sys.argv) > 2):
    fm_num = int(sys.argv[2])
  if (len(sys.argv) > 3):
    out_dir = sys.argv[3]
  if (len(sys.argv) > 4):
    batch = int(sys.argv[4])
  if (len(sys.argv) > 5):
    tmp = int(sys.argv[5])
    if tmp == 1:
      load_to_disk = True
    elif tmp == 0:
      load_to_disk = False
    else:
      print('Please select load_to_disk by 0: False or 1: True')
      exit()

  if num > 4:
    test_video_path = util.download_video2('http://web.stanford.edu/~jamesh93/video/wild480p.mkv')
  else:
    test_video_path = util.download_video1(num, fm_num)

  print('Batch {:d}, #{:d} video, #{:d} format, outdir: {}'.format(batch, 
    num, fm_num, out_dir))

  ensure_clean_state(test_video_path, batch)
  start = now()
  start_mxnet_pipeline(test_video_path, out_dir, batch, load_to_disk)
  stop = now()
  delta = stop - start
  print('Total pipeline time is: {:.4f} s'.format(delta))

  timelist += '"total-time" : %f' % (delta)
  timelist += "}"
  outString = 'Timelist:' + json.dumps(timelist)
  print outString

  outFile = '{}/end2end_{}_{}_{}_{}.out'.format(out_dir, num, fm_num, 
    WORK_PACKET_SIZE, batch)
  with open(outFile, 'w') as ofs:
    ofs.write(outString)

