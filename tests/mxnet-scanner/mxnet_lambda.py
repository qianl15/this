######################################################
# -*- coding: utf-8 -*-
# File Name: mxnet_lambda.py
# Author: Qian Li
# Created Date: 2017-11-04
# Description: This file show how to integrate Lambda
# with Scanner. Adapted from Scanner example6
######################################################

from scannerpy import Database, Job, DeviceType, BulkJob
from scannerpy.stdlib import parsers
import numpy as np
import sys
import os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
import util
from timeit import default_timer as now
import math

# choose which video we wanted to download, and the format
# format 134 = 360p, 135 = 480p, 136 = 720p, 137 = 1080p
# By default, we download the third video with the lowest quality
def test_mxnet_lambda(server="0.0.0.0", path="/hello", batch = 1, num = 3, 
                      fm_num = 1, out_dir = './'):

  test_video_path = util.download_video(num, fm_num)
  print('#{:d} video, #{:d} format, outdir: {}'.format(num, fm_num, out_dir))
  print('Lambda server: {}, Lambda path: {}'.format(server, path))
  if util.have_gpu():
    device = DeviceType.GPU
  else:
    device = DeviceType.CPU


  with Database() as db:
    if not os.path.isfile('lambda_op/build/libmxnetlambda_op.so'):
      print('You need to build the custom op first: \n'
          '$ cd lambda_op; mkdir build && cd build; cmake ..; make')
      exit()

    # To load a custom op into the Scanner runtime, we use db.load_op to open the
    # shared library we compiled. If the op takes arguments, it also optionally
    # takes a path to the generated python file for the arg protobuf.
    db.load_op('lambda_op/build/libmxnetlambda_op.so', 'lambda_op/build/mxnetlambda_pb2.py')

    start = now()
    [input_table], failed = db.ingest_videos([ 
        ('test_mxnet_raw', test_video_path)], force=True)
    stop = now()
    delta = stop - start
    print('Time to ingest videos: {:.4f}s, fps: {:.4f}'.format(
      delta, input_table.num_rows() / delta))
    num_rows = input_table.num_rows()
    print('Number of frames in movie: {:d}'.format(num_rows))
    
    if len(failed) > 0:
      print('Failures:', failed)

    # Start to analyze the movie
    start = now()
    frame = db.ops.FrameInput()
    # Then we use our op just like in the other examples.
    classes = db.ops.MxnetLambda(
      frame = frame,
      batch = batch,
      device = device,
      server = server, path = path)
    output_op = db.ops.Output(columns=[classes])
    job = Job(
      op_args={
        frame: db.table('test_mxnet_raw').column('frame'),
        output_op: 'test_mxnet_lambda'
      }
    )
    bulk_job = BulkJob(output=output_op, jobs=[job])
    [output_table] = db.run(bulk_job, force=True, profiling=True)

    stop = now()
    delta = stop - start
    print('Batch: {:d} MXNet Lambda time: {:.4f}s, {:.1f} fps\n'.format(
        batch, delta, input_table.num_rows() / delta))

    video_classes = output_table.load(['class'], parsers.classes)

    # Loop over the column's rows. Each row is a tuple of the frame number and
    # value for that row.
    num_rows = 0
    for (frame_index, frame_classes) in video_classes:
      assert len(frame_classes) == 1
      assert frame_classes[0].shape[0] == 1
      print(frame_classes[0])
      num_rows += 1
    assert num_rows == db.table('test_mxnet_raw').num_rows()

    print(db.summarize())

if __name__ == '__main__':
  num = 1 # which video
  fm_num = 1 # which resolution
  out_dir = './' # which output directory
  server="0.0.0.0" # lambda api gateway address
  path="/hello" # lambda function address
  batch = 1

  if (len(sys.argv) < 3) or (len(sys.argv) > 7):
    print('Usage: python mxnet_lambda.py <server> <path> (<batch_size> <video_num> <video_resolution> <out_dir>)');
    exit()

  server = sys.argv[1]
  path = sys.argv[2]

  if (len(sys.argv) > 3):
    batch = int(sys.argv[3])
  if (len(sys.argv) > 4):
    num = int(sys.argv[4])
  if (len(sys.argv) > 5):
    fm_num = int(sys.argv[5])
  if (len(sys.argv) > 6):
    out_dir = sys.argv[6]

  test_mxnet_lambda(server, path, batch, num, fm_num, out_dir)
