######################################################
# -*- coding: utf-8 -*-
# File Name: shot_detect.py
# Author: Qian Li
# Created Date: 2017-10-21
# Description: Shot detection, it can locate different
# shots from a video.
# Modified from Scanner/examples/shot_detection/
######################################################
from scannerpy import Database, DeviceType, Job, BulkJob
from scannerpy.stdlib import parsers
from scipy.spatial import distance
from subprocess import check_call as run
import numpy as np
import cv2
import math
import sys
import os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')
import util
from timeit import default_timer as now

WINDOW_SIZE = 500

def compute_shot_boundaries(hists):
  # Compute the mean difference between each pair of adjacent frames
  diffs = np.array(
            [np.mean([distance.chebyshev(hists[i-1][j], hists[i][j])
              for j in range(3)])
            for i in range(1, len(hists))])
  diffs = np.insert(diffs, 0, 0)
  n = len(diffs)

  # Do simple outlier detection to find boundaries between shots
  boundaries = []
  for i in range(1, n):
    window = diffs[max(i-WINDOW_SIZE,0):min(i+WINDOW_SIZE,n)]
    if diffs[i] - np.mean(window) > 3 * np.std(window):
      boundaries.append(i)
  return boundaries

def main(num = 1, fm_num = 1):
  
  movie_path = util.download_video(num, fm_num)
  print('Detecting shots in movie {}'.format(movie_path))
  movie_name = 'shot_detect'

  # Use GPU kernels if we have a GPU
  if util.have_gpu():
    device = DeviceType.GPU
  else:
    device = DeviceType.CPU

  device = DeviceType.CPU

  with Database() as db:
    print('Loading movie into Scanner DB...')
    total_time = 0.0
    start = now()

    ############ ############ ############ ############
    # 0. Ingest the video into the database
    ############ ############ ############ ############
    [movie_table], _ = db.ingest_videos([(movie_name, movie_path)],
                      force=True)
    stop = now()
    total_time += stop - start
    print('Ingest time: {:.4f}s '.format(stop - start))
    print('Number of frames in movie: {:d}'.format(movie_table.num_rows()))

    start = now()
    ############ ############ ############ ############
    # 1. Run Histogram over the entire video in Scanner
    ############ ############ ############ ############
    frame = db.ops.FrameInput()
    histogram = db.ops.Histogram(
      frame = frame,
      device = device)
    output = db.ops.Output(columns=[histogram])
    job = Job(op_args={
      frame: movie_table.column('frame'),
      output: movie_name + '_hist'
    })
    bulk_job = BulkJob(output=output, jobs=[job])
    [hists_table] = db.run(bulk_job, force=True)

    stop = now()
    total_time += stop - start
    print('Compute histogram time: {:.4f}s, {:.1f} fps'.format(
      stop - start, movie_table.num_rows() / (stop - start)))

    hists_table.profiler().write_trace('shot_detect_hist.trace')

    start = now()
    ############ ############ ############ ############
    # 2. Load histograms and compute shot boundaries
    #  in python
    ############ ############ ############ ############
    # Read histograms from disk
    hists = [h for _, h in hists_table.load(['histogram'],
                        parsers.histograms)]
    boundaries = compute_shot_boundaries(hists)
    stop = now()
    total_time += stop - start
    print('Found {:d} shots.'.format(len(boundaries)))
    print('Find boundaries time: {:.4f}s'.format(stop - start))

    start = now()
    ############ ############ ############ ############
    # 3. Create montage in Scanner
    ############ ############ ############ ############

    row_length = 16
    rows_per_item = 1
    target_width = 256
    item_size = row_length * rows_per_item

    # Compute partial row montages that we will stack together
    # at the end
    frame = db.ops.FrameInput()
    gather_frame = frame.sample()
    sliced_frame = gather_frame.slice()
    montage = db.ops.Montage(
      frame = sliced_frame,
      num_frames = item_size,
      target_width = target_width,
      frames_per_row = row_length,
      device = device)
    sampled_montage = montage.sample()
    output = db.ops.Output(
      columns=[sampled_montage.unslice().lossless()])

    starts_remainder = len(boundaries) % item_size
    evenly_divisible = (starts_remainder == 0)
    if not evenly_divisible:
      boundaries = boundaries[0:len(boundaries) - starts_remainder]

    job = Job(op_args={
      frame: movie_table.column('frame'),
      gather_frame: db.sampler.gather(boundaries),
      sliced_frame: db.partitioner.all(item_size),
      sampled_montage: [db.sampler.gather([item_size - 1])
                for _ in range(len(boundaries) / item_size)],
      output: 'montage_image'
    })

    bulk_job = BulkJob(output=output, jobs=[job])

    [montage_table] = db.run(bulk_job, force=True)
    
    # Stack all partial montages together
    montage_img = np.zeros((1, target_width * row_length, 3), dtype=np.uint8)
    for idx, img in montage_table.column('montage').load():
      img = np.flip(img, 2)
      montage_img = np.vstack((montage_img, img))

    stop = now()
    total_time += stop - start
    print('Create Montage time: {:.4f}s'.format(stop - start))
    montage_table.profiler().write_trace('shot_detect_montage.trace')

    start = now()
    ############ ############ ############ ############
    # 4. Write montage to disk
    ############ ############ ############ ############
    cv2.imwrite(
      'detected_shots_{:d}_{:d}.jpg'.format(num, fm_num), montage_img)
    stop = now()
    total_time += stop - start
    print('Successfully generated detected_shots.jpg')
    print('Write image time: {:.4f}s'.format(stop - start))
    print('Total time: {:.4f}s'.format(total_time))

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
  main(num, fm_num)
