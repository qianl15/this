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
import math

WORK_PACKET_SIZE = 50
def mean(arr):
  agg = 0.0
  for x in arr:
    agg += x
  return agg / len(arr)

def var(arr):
  arr_mean = mean(arr)
  agg = 0.0
  for x in arr:
    agg += (arr_mean - x) ** 2
  return agg / len(arr)

def std_var(arr):
  arr_var = var(arr)
  return math.sqrt(arr_var)

# choose which video we wanted to download, and the format
# format 134 = 360p, 135 = 480p, 136 = 720p, 137 = 1080p
# By default, we download the first video with the lowest quality
def test_histogram(n = 1, num = 1, fm_num = 1, out_dir = './'):
  if num > 4:
    test_video_path = util.download_video2('http://web.stanford.edu/~jamesh93/video/wild480p.mkv')
  else:
    test_video_path = util.download_video1(num, fm_num)

  # for videos stored on S3:
  #test_video_path = 'videos/example3_134.mp4'
  
  print('Total iterations: {:d}, #{:d} video, #{:d} format, outdir: {}'.format(n, num, fm_num, out_dir))
  outfile_name = out_dir + 'output_hist_{}_{}.out'.format(num, fm_num)
  f = open(outfile_name, 'w')
  f.write('Phase\tAvg Time(s)\t Std Var(s)\t n\t fps\n')
  # Use GPU kernels if we have a GPU
  if util.have_gpu():
    device = DeviceType.GPU
  else:
    device = DeviceType.CPU

  device = DeviceType.CPU

  with Database() as db:
    total_time = []
    total_ingest_time = []
    total_hist_time = []
    num_rows = 0

    for iter in xrange(n):
      # test time of ingest
      start = now()
      [input_table], failed = db.ingest_videos([ 
          ('test_hist_raw', test_video_path)], force=True)
      stop = now()
      delta = stop - start
      total_time.append(delta)
      total_ingest_time.append(delta)
      print('Time to ingest videos: {:.4f}s, fps: {:.4f}'.format(
        delta, input_table.num_rows() / delta))
      num_rows = input_table.num_rows()
      print('Number of frames in movie: {:d}'.format(num_rows))
      
      if len(failed) > 0:
       print('Failures:', failed)

      start = now()
      # test execution time
      frame = db.ops.FrameInput()
      histogram = db.ops.Histogram(
        frame = frame,
        device = device,
        batch = 10)
      output = db.ops.Output(columns=[histogram])
      job = Job(op_args={
        frame: input_table.column('frame'),
        output: 'test_hist_hist'
      })
      bulk_job = BulkJob(output=output, jobs=[job])
      [hists_table] = db.run(bulk_job, force=True, profiling=True, show_progress=True, work_packet_size=WORK_PACKET_SIZE, pipeline_instances_per_node=8)

      stop = now()
      delta = stop - start
      total_time[iter] += delta
      total_hist_time.append(delta)
      print('Compute histogram time: {:.4f}s, {:.1f} fps\n'.format(
        delta, input_table.num_rows() / delta))

      # only write out the last one trace will be good
      if iter == n - 1:
        hists_table.profiler().write_trace(
          out_dir + 'test_hist_{:d}_{:d}.trace'.format(num, fm_num))

    avg_ingest_time = mean(total_ingest_time)
    avg_hist_time = mean(total_hist_time)
    avg_time = mean(total_time)
    std_var_ingest = std_var(total_ingest_time)
    std_var_hist = std_var(total_hist_time)
    std_var_time = std_var(total_time)

    print('Avg time to ingest videos: {:.4f}s (stdvar={:.4f}s, n={:d}), fps: {:.4f}'.format(
        avg_ingest_time, std_var_ingest, n, num_rows / avg_ingest_time))
    print('Avg time to compute histogram: {:.4f}s (stdvar={:.4f}s, n={:d}), {:.1f} fps\n'.format(
        avg_hist_time, std_var_hist, n, num_rows / avg_hist_time))
    print('Avg total time: {:.4f}s (stdvar={:.4f}s, n={:d}), total {:.1f} fps\n'.format(
        avg_time, std_var_time, n, num_rows / avg_time))

    f.write('Ingest\t {:.6f} \t {:.6f} \t {:d} \t{:.1f}\n'.format(
        avg_ingest_time, std_var_ingest, n, num_rows / (avg_ingest_time)))
    f.write('Histogram\t {:.6f} \t {:.6f} \t {:d} \t{:.1f}\n'.format(
        avg_hist_time, std_var_hist, n, num_rows / avg_hist_time))
    f.write('Total\t {:.6f} \t {:.6f} \t {:d} \t{:.1f}\n'.format(
        avg_time, std_var_time, n, num_rows / avg_time))
    print(db.summarize())

  return


if __name__ == "__main__":
  n = 1 # number of iterations
  num = 1 # which video
  fm_num = 1 # which resolution
  out_dir = './' # which output directory
  # usage: python histogram.py n num fm_num out_dir
  if len(sys.argv) > 1:
    n = int(sys.argv[1])
    if len(sys.argv) > 2:
      num = int(sys.argv[2])
    if len(sys.argv) > 3:
      fm_num = int(sys.argv[3])
    if len(sys.argv) > 4:
      out_dir = sys.argv[4]
    if len(sys.argv) > 5:
      print('Please enter at most four parameters')
      exit()
  test_histogram(n, num, fm_num, out_dir)
