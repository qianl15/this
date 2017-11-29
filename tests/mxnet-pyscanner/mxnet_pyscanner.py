########################################################
# -*- coding: utf-8 -*-
# File Name: mxnet_pyscanner.py
# Author: Qian Li
# Created Date: 2017-11-07
# Description: This file shows how to use a python kernel
#########################################################

from scannerpy import Database, Job, ColumnType, DeviceType, BulkJob
from scannerpy.stdlib import parsers
import numpy as np
import sys
import os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
import util
from timeit import default_timer as now
import math
from urllib import urlretrieve

WORK_PACKET_SIZE = 50
# choose which video we wanted to download, and the format
# format 134 = 360p, 135 = 480p, 136 = 720p, 137 = 1080p
# By default, we download the third video with the lowest quality
def test_pymxnet(num = 3, fm_num = 1, out_dir = './', batch = 1):

  if num > 4:
    test_video_path = util.download_video2('http://web.stanford.edu/~jamesh93/video/wild480p.mkv')
  else:
    test_video_path = util.download_video1(num, fm_num)

  print('#{:d} video, #{:d} format, outdir: {}'.format(num, fm_num, out_dir))
  if util.have_gpu():
    device = DeviceType.GPU
  else:
    device = DeviceType.CPU

  script_dir = os.path.dirname(os.path.abspath(__file__))

  with Database() as db:
    # if not os.path.isfile('pymxnet_op/build/libpymxnet_op.so'):
    #   print('You need to build the custom op first: \n'
    #       '$ cd pymxnet_op; mkdir build && cd build; cmake ..; make')
    #   exit()

    # # To load a custom op into the Scanner runtime, we use db.load_op to open the
    # # shared library we compiled. If the op takes arguments, it also optionally
    # # takes a path to the generated python file for the arg protobuf.
    # db.load_op('pymxnet_op/build/libpymxnet_op.so', 'pymxnet_op/build/pymxnet_pb2.py')
    db.register_op('PyMxnet', [('frame', ColumnType.Video)], ['class'])
    kernel_path = script_dir + '/pymxnet_op/pymxnet_op.py'
    db.register_python_kernel('PyMxnet', DeviceType.CPU, kernel_path, batch=10)

    start = now()
    [input_table], failed = db.ingest_videos([ 
        ('test_pymxnet_raw', test_video_path)], force=True)
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
    classes = db.ops.PyMxnet(frame = frame, batch = batch)
    output_op = db.ops.Output(columns=[classes])
    job = Job(
      op_args={
        frame: db.table('test_pymxnet_raw').column('frame'),
        output_op: 'test_pymxnet_out'
      }
    )
    bulk_job = BulkJob(output=output_op, jobs=[job])
    [output_table] = db.run(bulk_job, force=True, profiling=True, pipeline_instances_per_node=1, work_packet_size=WORK_PACKET_SIZE)

    stop = now()
    delta = stop - start
    print('Batch: {:d} Python MXNet time: {:.4f}s, {:.1f} fps\n'.format(
        batch, delta, input_table.num_rows() / delta))

    output_table.profiler().write_trace(
      out_dir + 'test_pymxnet_{:d}_{:d}.trace'.format(num, fm_num))

    video_classes = output_table.load(['class'], parsers.classes)

    # Loop over the column's rows. Each row is a tuple of the frame number and
    # value for that row.
    num_rows = 0
    for (frame_index, frame_classes) in video_classes:
      assert len(frame_classes) == 1
      assert frame_classes[0].shape[0] == 1
      print(frame_classes[0])
      num_rows += 1
    assert num_rows == db.table('test_pymxnet_raw').num_rows()

    print(db.summarize())

if __name__ == '__main__':
  num = 1 # which video
  fm_num = 1 # which resolution
  out_dir = './' # which output directory
  batch = 1

  f_params = 'resnet-18-0000.params'
  f_symbol = 'resnet-18-symbol.json'

  #params
  f_params_file = '/tmp/' + f_params
  urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-0000.params", f_params_file)

  #symbol
  f_symbol_file = '/tmp/' + f_symbol
  urlretrieve("https://s3-us-west-2.amazonaws.com/mxnet-params/resnet-18-symbol.json", f_symbol_file)

  if (len(sys.argv) < 1) or (len(sys.argv) > 5):
    print('Usage: python mxnet_pyscanner.py <video_num> <video_resolution> <out_dir> <batch_size>');
    exit()

  if (len(sys.argv) > 1):
    num = int(sys.argv[1])
  if (len(sys.argv) > 2):
    fm_num = int(sys.argv[2])
  if (len(sys.argv) > 3):
    out_dir = sys.argv[3]
  if (len(sys.argv) > 4):
    batch = int(sys.argv[4])

  test_pymxnet(num, fm_num, out_dir, batch)
