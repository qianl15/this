######################################################
# -*- coding: utf-8 -*-
# File Name: histogram.py
# Author: Qian Li
# Created Date: 2017-10-20
# Description: This file is the basic sample end-to-end
# pipeline that ingests a video into Scanner, compute
# the histogram, and then extract the result.
# And it will also save the trace file
# Modified from Scanner/examples/tutorial/*
######################################################

from scannerpy import Database, Job, DeviceType, BulkJob
from scannerpy.stdlib import parsers
import numpy as np
import cv2
import sys
import os.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import util
from timeit import default_timer as now

with Database() as db:
  # Create a Scanner table from our video in the format (table name,
  # video path). If any videos fail to ingest, they'll show up in the failed
  # list. If force is true, it will overwrite existing tables of the same
  # name.
  example_video_path1 = util.download_video(1)
  example_video_path2 = util.download_video(2)

  # test time of ingest
  start = now()
  input_tables, failed = db.ingest_videos([
      ('test_raw1', example_video_path1), 
      ('test_raw2', example_video_path2)], force=True)
  print('Time to ingest videos: {:.6f}s'.format(now() - start))
  print(db.summarize())
  print('Failures:', failed)

  # Scanner processes videos by forming a graph of operations that operate
  # on input frames from a table and produce outputs to a new table.

  # FrameInput declares that we want to read from a table column that
  # represents a video frame.
  frame = db.ops.FrameInput()

  # These frames are input into a Histogram op that computes a color histogram
  # for each frame.
  hist = db.ops.Histogram(frame=frame)

  # Finally, any columns provided to Output will be saved to the output
  # table at the end of the computation.
  output_op = db.ops.Output(columns=[hist])

  # A job defines a table you want to create. In op_args, we bind the frame
  # input column from above to the table we want to read from and name
  # the output table 'example_hist' by binding a string to output_op.
  job1 = Job(
    op_args={
      frame: db.table('test_raw1').column('frame'),
      output_op: 'test_hist1'
    }
  )

  job2 = Job(
    op_args={
      frame: db.table('test_raw2').column('frame'),
      output_op: 'test_hist2'
    }
  )

  # Multiple tables can be created using the same execution graph using
  # a bulk job. Here we specify the execution graph (or DAG) by providing
  # the output_op and also specify the jobs we wish to compute.
  bulk_job = BulkJob(output=output_op, jobs=[job1, job2])

  # This executes the job and produces the output table. You'll see a progress
  # bar while Scanner is computing the outputs.
  start = now()
  output_tables = db.run(bulk_job, force=True)
  print('Totaltime to decode + compute histograms: {:.6f}s, including profiling time'.format(now() - start))
  
  num = 1
  for output_table in output_tables:
    # The profiler contains information about how long different parts of your
    # computation take to run. We use Google Chrome's trace format, which you
    # can view by going to chrome://tracing in Chrome and clicking "load" in
    # the top left.
    # Since scanner will run bulk jobs in one pipeline, the trace file for
    # jobs are combined into one giant file for the whole bulk jobs
    if num == 1:
     output_table.profiler().write_trace('test_hist.trace'.format(num))
    # Each row corresponds to a different part of the system, e.g. the thread
    # loading bytes from disk or the thread running your kernels. If you have
    # multiple pipelines or multiple nodes, you will see many of these evaluate
    # threads.


    # Load the histograms from a column of the output table. The
    # parsers.histograms  function  converts the raw bytes output by Scanner
    # into a numpy array for each channel.
    video_hists = output_table.load(['histogram'], parsers.histograms)

    # Loop over the column's rows. Each row is a tuple of the frame number and
    # value for that row.
    num_rows = 0
    for (frame_index, frame_hists) in video_hists:
        assert len(frame_hists) == 3
        assert frame_hists[0].shape[0] == 16
        num_rows += 1
    assert num_rows == db.table('test_raw{:d}'.format(num)).num_rows()
    num += 1

  print(db.summarize())
