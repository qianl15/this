######################################################
# -*- coding: utf-8 -*-
# File Name: plot_log_data.py
# Author: James Hong & Qian Li
# Created Date: 2017-11-29
# Description: Plot data from parsed logs
######################################################
#!/usr/bin/env python

import json
import argparse
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.font_manager import FontProperties

LABEL_FONT_SIZE = 12
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE, weight='bold')

def get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--data', '-d', type=str, required=True,
            help='Data collected by parser script')
  return parser.parse_args()


def plot_histogram(data, title, xlabel, outfile, color='red', nbins=300,
                   xmin=0, xmax=1800):
  # the histogram of the data
  axes = plt.gca()
  axes.set_xlim([xmin, xmax])

  step = (xmax - xmin) / nbins
  nbinsList = xrange(xmin, xmax, step)
  plt.hist(data, nbinsList, facecolor=color, alpha=0.85, zorder=3)
  plt.xlabel(xlabel,fontproperties=LABEL_FP)
  plt.ylabel('Count', fontproperties=LABEL_FP)

  # draw grid behind bars
  plt.grid(True, zorder=0, linestyle='dashed', linewidth=0.5) 

  mean = np.mean(data)
  median = np.median(data)
  plt.title('{}: mean={:.2f}, median={:.2f}'.format(title, mean, median))
  
  plt.savefig(outfile)
  plt.clf() # remember to clear!


def main(args):
  with open(args.data, 'r') as ifs:
    data = json.load(ifs)

  if "duration" in data:
    duration_data = [x/100 for x in data['duration']]
    plot_histogram(duration_data, 'Lambda duration', 'Time (100 ms)',
                   'duration_{}.pdf'.format(args.data))

  if "billed-duration" in data:
    billed_data = [x/100 for x in data['billed-duration']]
    plot_histogram(billed_data, 'Lambda billed duration',
                  'Time (100 ms)', 'billed-duration_{}.pdf'.format(args.data))

  if "start-time" in data:
    plot_histogram(data['start-time'], 'Lambda start time', 
      'UNIX Timestamp (s)', 'start-time_{}.pdf'.format(args.data), 
      xmin=int(min(data['start-time'])), xmax=int(max(data['start-time'])),
      nbins=100)

  if "end-time" in data:
    plot_histogram(data['end-time'], 'Lambda end time', 
      'UNIX Timestamp (s)', 'end-time_{}.pdf'.format(args.data), 
      xmin=int(min(data['end-time'])), xmax=int(max(data['end-time'])),
      nbins=100)

  # TODO: add more fields



if __name__ == '__main__':
  main(get_args())
