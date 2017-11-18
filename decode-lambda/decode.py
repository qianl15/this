#!/usr/bin/env python

import boto3
import botocore
import hashlib
import os
import shutil
import struct 
import subprocess
from multiprocessing.pool import ThreadPool
from threading import Semaphore
import urllib
from timeit import default_timer as now

TEMP_OUTPUT_DIR = '/tmp/output'

INPUT_FILE_PATH = os.path.join('/tmp', 'input')

FFMPEG_PATH = '/tmp/ffmpeg'
shutil.copyfile('ffmpeg', FFMPEG_PATH)
os.chmod(FFMPEG_PATH, 0o0755)

MIN_DECODE_QUALITY = 2
MAX_DECODE_QUALITY = 31

DEFAULT_DECODE_QUALITY = 5
DEFAULT_DECODE_FPS = 24
DEFAULT_LOG_LEVEL = 'warning'

DEFAULT_OUTPUT_BATCH_SIZE = 1
DEFAULT_KEEP_OUTPUT = False

MAX_PARALLEL_UPLOADS = 20

OUTPUT_FILE_EXT = 'jpg'

def get_md5(filePath):
    hash_md5 = hashlib.md5()
    with open(filePath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def convert_video_to_jpegs(fps, quality, logLevel):
    print 'Decoding at %d fps with quality %d' % (fps, quality)
    cmd = [
        FFMPEG_PATH, 
        '-i', INPUT_FILE_PATH, 
        '-v', logLevel,
        '-xerror',
        '-q:v', str(quality),
        '-vf',
        'fps=%d' % fps, 
        os.path.join(TEMP_OUTPUT_DIR, '%04d.{0}'.format(OUTPUT_FILE_EXT))
    ]
    process = subprocess.Popen(
        ' '.join(cmd), shell=True,
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE)
    out, err = process.communicate()
    rc = process.returncode
    print 'stdout:', out
    print 'stderr:', err
    return rc == 0


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
    if os.path.exists(INPUT_FILE_PATH):
        os.remove(INPUT_FILE_PATH)
    if os.path.exists(TEMP_OUTPUT_DIR):
        shutil.rmtree(TEMP_OUTPUT_DIR)


def handler(event, context):
    ensure_clean_state()

    videoUrl = event['videoUrl']

    if 'decodeFps' in event:
        decodeFps = int(event['decodeFps'])
    else:
        decodeFps = DEFAULT_DECODE_FPS

    if 'decodeQuality' in event:
        decodeQuality = int(event['decodeQuality'])
    else:
        decodeQuality = DEFAULT_DECODE_QUALITY
    if decodeQuality < MIN_DECODE_QUALITY or \
        decodeQuality > MAX_DECODE_QUALITY:
        raise Exception('Invalid decode quality: %d', decodeQuality)

    logLevel = DEFAULT_LOG_LEVEL
    if 'logLevel' in event:
        logLevel = event['logLevel']

    outputBucket = None
    if 'outputBucket' in event:
        outputBucket = event['outputBucket']
        outputPrefix = event['outputPrefix']
    else:
        print 'Warning: no output location specified'

    outputBatchSize = DEFAULT_OUTPUT_BATCH_SIZE
    if 'outputBatchSize' in event:
        outputBatchSize = int(event['outputBatchSize'])

    keepOutput = DEFAULT_KEEP_OUTPUT
    if 'keepOutput' in event:
        keepOutput = event['keepOutput'].lower() == 'true'

    print 'Downloading file: %s' % videoUrl
    urllib.urlretrieve(videoUrl, INPUT_FILE_PATH)

    print 'Download complete'
    if not os.path.exists(INPUT_FILE_PATH):
        raise Exception('%s does not exist' % INPUT_FILE_PATH)
    else:
        inputSize = os.path.getsize(INPUT_FILE_PATH)
        os.chmod(INPUT_FILE_PATH, 0o0755)
        print ' [%dKB] %s' % (inputSize >> 10, INPUT_FILE_PATH)
        print ' [md5] %s' % get_md5(INPUT_FILE_PATH)

    os.mkdir(TEMP_OUTPUT_DIR)
    assert(os.path.exists(TEMP_OUTPUT_DIR))
    try:
        try:
            if not convert_video_to_jpegs(decodeFps, decodeQuality, logLevel):
                raise Exception('Failed to decode video')
        finally:
            os.remove(INPUT_FILE_PATH)

        if outputBatchSize > 1:
            combine_output_files(outputBatchSize)

        if outputBucket:
            fileCount, totalSize = upload_output_to_s3(
                outputBucket, outputPrefix)
        else:
            fileCount, totalSize = list_output_directory()
    finally:
        if not keepOutput:
            shutil.rmtree(TEMP_OUTPUT_DIR)

    return {
        'statusCode': 200,
        'body': {
            'fileCount': fileCount,
            'totalSize': totalSize
        }
    }


if __name__ == '__main__':
    # TODO: probably want to be able to take files from S3 too
    event = {
        'videoUrl': 'http://web.stanford.edu/~jamesh93/video/wild480p.mkv',
        'outputBucket': 'vass-video-samples2',
        'outputPrefix': 'jpeg-test',
        'decodeFps': 30,
        'outputBatchSize': 100,
        # 'keepOutput': 'true'
    }
    start = now()
    handler(event, {})
    stop = now()
    delta = stop - start
    print('Time to decode is: {:.4f}s'.format(delta))
