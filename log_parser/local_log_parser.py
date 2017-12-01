#!/usr/bin/env python
######################################################
# -*- coding: utf-8 -*-
# File Name: local_log_parser.py
# Author: James Hong & Qian Li
# Created Date: 2017-10-28
# Description: Parse local logs
######################################################

import argparse
import json
import os
import re
import sys
import time

# need to install python-dateutil
import dateutil.parser

import numpy as np

from collections import OrderedDict

def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--logdir', '-l', type=str, required=True,
            help='Directory where logs files are stored')
  parser.add_argument('--outfile', '-o', type=str, required=True,
            help='File to save parsed output')
  return parser.parse_args()


class StatsObject(object):

  def __init__(self):
    self.numLambdas = 0
    self.totalLogs = 0
    self.data = OrderedDict()

  def incrementNumLambdas(self):
    self.numLambdas += 1

  def record_key_value(self, k, v):
    if k not in self.data:
      self.data[k] = []
    self.data[k].append(v)

  def print_stats(self):
    print 'Parsed %d lambda logs out of %d logs' % (self.numLambdas, 
                                                    self.totalLogs)
    for k, v in self.data.iteritems():
      print k
      print '  mean:', np.mean(v)
      print '  stdev:', np.std(v)
      print '  median:', np.median(v)
      print '  min:', min(v)
      print '  max:', max(v)
      print '  10th:', np.percentile(v, 10)
      print '  25th:', np.percentile(v, 25)
      print '  75th:', np.percentile(v, 75)
      print '  90th:', np.percentile(v, 90)
      print '  95th:', np.percentile(v, 95)
      print '  99th:', np.percentile(v, 99)

  def dump_parsed_values(self, outfile):
    print >> sys.stderr, 'Writing parsed results to', outfile
    with open(outfile, 'w') as ofs:
      json.dump(self.data, ofs)


REPORT_RE = re.compile(r'Duration: ([\d.]+) ms\s+Billed Duration: (\d+) ms\s+Memory Size: (\d+) MB\s+Max Memory Used: (\d+) MB')

def parse_line(line, stats):
  if 'Timelist:' in line:
    timelistObj = None
    # back compatibility, support two types
    try:
      _, timelist = line.split('Timelist:', 1)
      timelistObj = json.loads(json.loads(timelist.strip()))
    except Exception as e:
      try:
        timelistObj = json.loads(timelist)
      except Exception as e:
        print >> sys.stderr, e, line
      
    for k, v in timelistObj.iteritems():
      stats.record_key_value(k, v)
    stats.totalLogs += 1
  
  if 'START' in line:
    timeStr, _ = line.split(' ', 1)  
    parsedDate = dateutil.parser.parse(timeStr)
    stats.record_key_value('start-time', time.mktime(parsedDate.timetuple()))

  if 'END' in line:
    timeStr, _ = line.split(' ', 1)
    parsedDate = dateutil.parser.parse(timeStr)
    stats.record_key_value('end-time', time.mktime(parsedDate.timetuple()))

  matchObj = REPORT_RE.search(line)
  if matchObj is not None:
    duration = float(matchObj.group(1))
    billedDuration = int(matchObj.group(2))
    memorySize = int(matchObj.group(3))
    maxMemoryUsed = int(matchObj.group(4))

    stats.record_key_value('duration', duration)
    stats.record_key_value('billed-duration', billedDuration)
    stats.record_key_value('memory-size', memorySize)
    stats.record_key_value('max-memory-used', maxMemoryUsed)
    stats.incrementNumLambdas()


def main(args):
  if not os.path.isdir(args.logdir):
    raise Exception('Log directory does not exist')

  stats = StatsObject()

  # recursively walk subdirs
  for dirpath, dirnames, filenames in os.walk(args.logdir):
    for fileName in filenames:
      filePath = os.path.join(dirpath, fileName)
      print >> sys.stderr, 'Parsing', fileName
      try:
        with open(filePath, 'r') as ifs:
          for line in ifs:
            parse_line(line, stats)
      except Exception as e:
        print >> sys.stderr, e

  stats.print_stats()
  if args.outfile is not None:
    stats.dump_parsed_values(args.outfile)


if __name__ == '__main__':
  main(get_args())
