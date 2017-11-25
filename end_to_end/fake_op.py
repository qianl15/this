######################################################
# -*- coding: utf-8 -*-
# File Name: fake_op.py
# Author: Qian Li
# Created Date: 2017-11-24
# Description: An empty op to avoid computation
######################################################
import scannerpy
import struct

class FakeKernel(scannerpy.Kernel):
  def __init__(self, config, protobufs):
    self.protobufs = protobufs
    pass

  def close(self):
    pass

  def execute(self, input_columns):
    input_count = len(input_columns[0])
    column_count = len(input_columns)
    return [[struct.pack('=i', 233) for _ in xrange(input_count)] 
             for _ in xrange(column_count)]

KERNEL = FakeKernel