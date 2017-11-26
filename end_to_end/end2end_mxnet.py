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
import math
from urllib import urlretrieve

WORK_PACKET_SIZE = 200  # how many frames to decode together
BATCH_SIZE = 50

# choose which video we wanted to download, and the format
# format 134 = 360p, 135 = 480p, 136 = 720p, 137 = 1080p
# By default, we download the third video with the lowest quality
# batch - number of frames to do in a MXNet Lambda
def start_mxnet_pipeline(test_video_path='videos/example.mp4', 
                         out_dir = './', batch = BATCH_SIZE,
                         load_to_disk = False):
  
  if util.have_gpu():
    device = DeviceType.GPU
    print('with GPU device!')
  else:
    device = DeviceType.CPU
    print('only has CPU device!')

  script_dir = os.path.dirname(os.path.abspath(__file__))

  # Start Scanner DB, use its load worker to generate .proto and .bin files
  with Database() as db:
    # register the fake kernel
    db.register_op('Fake', [('frame', ColumnType.Video)], ['class'])
    kernel_path = script_dir + '/fake_op.py'
    db.register_python_kernel('Fake', device, kernel_path, batch = 10)

    start = now()
    [input_table], failed = db.ingest_videos([ 
        ('end2end_raw', test_video_path)], force=True)
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
    classes = db.ops.Fake(frame = frame, batch = batch)
    output_op = db.ops.Output(columns=[classes])
    job = Job(
      op_args={
        frame: input_table.column('frame'),
        output_op: 'end2end_out'
      }
    )
    bulk_job = BulkJob(output=output_op, jobs=[job])
    [output_table] = db.run(bulk_job, force=True, profiling=True, pipeline_instances_per_node=1, load_to_disk=load_to_disk, 
      work_packet_size=WORK_PACKET_SIZE)

    stop = now()
    delta = stop - start
    print('Batch: {:d} End-to-end Fake Python Kernel time: {:.4f}s, {:.1f} fps\n'.format(batch, delta, input_table.num_rows() / delta))

    output_table.profiler().write_trace(
      out_dir + 'end2end_{:d}.trace'.format(batch))

    if load_to_disk == False:
      video_classes = output_table.load(['class'], parsers.classes)

      # Loop over the column's rows. Each row is a tuple of the frame number and
      # value for that row.
      num_rows = 0
      for (frame_index, frame_classes) in video_classes:
        assert len(frame_classes) == 1
        assert frame_classes[0].shape[0] == 1
        # print(frame_classes[0])
        num_rows += 1
      assert num_rows == db.table('end2end_raw').num_rows()

      print(db.summarize())

  # Then start the Lambda part
  if load_to_disk == True:
    pass

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

  start_mxnet_pipeline(test_video_path, out_dir, batch, load_to_disk)