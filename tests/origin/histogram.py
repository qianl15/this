######################################################
# -*- coding: utf-8 -*-
# File Name: histogram.py
# Author: Qian Li
# Created Date: 2017-10-24
# Description: Test the performance of computing
# histogram with Scanner
######################################################

from scannerpy import Database, Job, DeviceType, BulkJob
from scannerpy.stdlib import parsers
import numpy as np
import cv2
import sys
import os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
import util
from timeit import default_timer as now

# choose which video we wanted to download, and the format
# format 134 = 360p, 135 = 480p, 136 = 720p, 137 = 1080p
# By default, we download the first video with the lowest quality
def test_histogram(num = 1, fm_num = 1):
  test_video_path = util.download_video(num, fm_num)
  outfile_name = 'output_hist_{}_{}.out'.format(num, fm_num)
  f = open(outfile_name, 'w')
  f.write('Phase\tTime(s)\tfps\n')
  # Use GPU kernels if we have a GPU
  if util.have_gpu():
    device = DeviceType.GPU
  else:
    device = DeviceType.CPU

  device = DeviceType.CPU

  with Database() as db:
    total_time = 0.0

    # test time of ingest
    start = now()
    [input_table], failed = db.ingest_videos([ 
        ('test_hist_raw', test_video_path)], force=True)
    stop = now()
    total_time += stop - start
    print('Time to ingest videos: {:.4f}s, fps: {:.4f}'.format(
      stop - start, input_table.num_rows() / (stop - start)))
    f.write('Ingest\t {:.6f} \t{:.1f}\n'.format(
      stop-start, input_table.num_rows() / (stop - start)))
    print('Number of frames in movie: {:d}'.format(input_table.num_rows()))
    
    if len(failed) > 0:
     print('Failures:', failed)

    start = now()
    # test execution time
    frame = db.ops.FrameInput()
    histogram = db.ops.Histogram(
      frame = frame,
      device = device)
    output = db.ops.Output(columns=[histogram])
    job = Job(op_args={
      frame: input_table.column('frame'),
      output: 'test_hist_hist'
    })
    bulk_job = BulkJob(output=output, jobs=[job])
    [hists_table] = db.run(bulk_job, force=True, profiling=True)

    stop = now()
    total_time += stop - start
    print('Compute histogram time: {:.4f}s, {:.1f} fps\n'.format(
      stop - start, input_table.num_rows() / (stop - start)))
    f.write('Histogram\t {:.6f} \t{:.1f}\n'.format(
      stop-start, input_table.num_rows() / (stop - start)))

    print('Total time: {:.4f}s, total {:.1f} fps\n'.format(
      total_time, input_table.num_rows() / total_time))
    f.write('Total\t {:.6f} \t{:.1f}\n'.format(
      total_time, input_table.num_rows() / total_time))
    hists_table.profiler().write_trace(
      'test_hist_{:d}_{:d}.trace'.format(num, fm_num))
    print(db.summarize())

  return


if __name__ == "__main__":
  num = 1
  fm_num = 1
  # The first param is the # of video
  # The second param is the # of format
  if len(sys.argv) > 1:
    if len(sys.argv) == 2:
      num = int(sys.argv[1])
    elif len(sys.argv) == 3:
      num = int(sys.argv[1])
      fm_num = int(sys.argv[2])
    else:
      print('Please enter at most two parameters')
      exit()
  test_histogram(num, fm_num)