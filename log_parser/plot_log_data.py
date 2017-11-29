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


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', '-d', type=str, required=True,
                        help='Data collected by parser script')
    return parser.parse_args()


def plot_histogram(data, title, xlabel, outfile, color='red', nbins=50):
    # the histogram of the data
    plt.hist(data, nbins, facecolor=color, alpha=0.75)
    plt.xlabel(xlabel)
    plt.ylabel('Count')
    plt.title(title)
    plt.grid(True)
    plt.savefig(outfile)


def main(args):
    with open(args.data, 'r') as ifs:
        data = json.load(ifs)

    plot_histogram(data['duration'], 'Lambda duration', 'Milliseconds',
                   'duration.pdf')

    plot_histogram(data['billed-duration'], 'Lambda billed duration',
                   'Milliseconds', 'billed-duration.pdf')

    # TODO: add more fields



if __name__ == '__main__':
    main(get_args())
